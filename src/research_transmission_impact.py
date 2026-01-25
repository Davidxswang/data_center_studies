import json
import math
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandapower as pp  # type: ignore[import-untyped]
import pandapower.networks as nw  # type: ignore[import-untyped]
from pandapower import pandapowerNet
from loguru import logger
from tqdm.rich import tqdm


def _run_pf_chunk(
    chunk_data: list[tuple[int, float, float]],  # List of (time_idx, p_mw, q_mvar)
    network_name: str,
    grid_bus_idx: int,
    use_lightsim2grid: bool,
) -> list[dict]:
    """
    Run power flow for a chunk of time steps. Designed to run in a separate process.

    Returns list of result dicts with voltage, losses, slack power for each time step.
    """
    # Create network in this process
    net: pandapowerNet = getattr(nw, network_name)()
    dc_load_idx = pp.create_load(net, bus=grid_bus_idx, p_mw=0, q_mvar=0, name="AI_Data_Center")

    # Find lines connected to the DC bus for loading analysis
    connected_lines = net.line[(net.line.from_bus == grid_bus_idx) | (net.line.to_bus == grid_bus_idx)].index.tolist()

    results = []
    for time_idx, p_mw, q_mvar in chunk_data:
        # Update load
        net.load.at[dc_load_idx, "p_mw"] = p_mw
        net.load.at[dc_load_idx, "q_mvar"] = q_mvar

        try:
            pp.runpp(net, lightsim2grid=use_lightsim2grid)
            results.append(
                {
                    "time_idx": time_idx,
                    "vm_pu": net.res_bus.at[grid_bus_idx, "vm_pu"],
                    "slack_p_mw": net.res_ext_grid["p_mw"].sum(),
                    "total_loss_mw": net.res_line["pl_mw"].sum(),
                    "max_line_loading": net.res_line.loc[connected_lines, "loading_percent"].max() if connected_lines else 0,
                    "converged": True,
                }
            )
        except pp.LoadflowNotConverged:
            results.append(
                {
                    "time_idx": time_idx,
                    "vm_pu": float("nan"),
                    "slack_p_mw": float("nan"),
                    "total_loss_mw": float("nan"),
                    "max_line_loading": float("nan"),
                    "converged": False,
                }
            )

    return results


def run_transmission_pf_timeseries_simulation(
    load_profile_path: Path,
    output_dir: Path,
    network_name: str = "case39",
    grid_bus_idx: int = 16,
    peak_load_mw: float = 1000.0,
    use_lightsim2grid: bool = True,
    num_workers: int | None = None,
) -> None:
    """
    Analyze the transmission grid impact of an AI data center load profile.

    Uses parallel processing to speed up power flow calculations.

    Args:
        load_profile_path: Path to the aggregated volatility profile CSV.
        output_dir: Path to the output directory.
        network_name: Name of the network to use.
        grid_bus_idx: Bus index on the network to connect the load.
        peak_load_mw: Peak power of the AI data center in MW.
        use_lightsim2grid: If True, use LightSim2Grid backend for faster power flow.
        num_workers: Number of parallel workers. Defaults to CPU count.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load and Scale Profile
    logger.info(f"Loading profile from {load_profile_path}")
    df_profile = pd.read_csv(load_profile_path)

    # Normalize profile (0 to 1) based on its own max
    profile_max = df_profile["power_mw"].max()
    df_profile["scaling_factor"] = df_profile["power_mw"] / profile_max

    # Scale to Target Peak (e.g., 1000 MW)
    df_profile["ai_load_mw"] = df_profile["scaling_factor"] * peak_load_mw

    # Calculate reactive power assuming PF=0.98 lagging (inductive)
    # Q = P * tan(acos(0.98)) ≈ P * 0.203
    pf = 0.98
    tan_phi = math.tan(math.acos(pf))
    df_profile["ai_load_mvar"] = df_profile["ai_load_mw"] * tan_phi

    # 2. Check lightsim2grid availability
    if use_lightsim2grid:
        try:
            import lightsim2grid  # type: ignore[import-untyped]

            logger.info(f"Using LightSim2Grid v{lightsim2grid.__version__} backend for faster power flow")
        except ImportError as e:
            logger.warning(f"LightSim2Grid not installed. Using default solver. {e}")
            raise e

    # 3. Prepare data for parallel execution
    steps = len(df_profile)
    if num_workers is None:
        num_workers = os.cpu_count() or 4

    logger.info(f"Starting parallel power flow simulation: {steps} steps with {num_workers} workers...")

    # Create list of (time_idx, p_mw, q_mvar) tuples
    pf_data = [(i, df_profile["ai_load_mw"].iloc[i], df_profile["ai_load_mvar"].iloc[i]) for i in range(steps)]

    # Split into chunks for each worker
    chunk_size = max(1, steps // num_workers)
    chunks = [pf_data[i : i + chunk_size] for i in range(0, steps, chunk_size)]

    # 4. Run power flows in parallel
    all_results: list[dict] = []

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(
                _run_pf_chunk,
                chunk,
                network_name,
                grid_bus_idx,
                use_lightsim2grid,
            ): i
            for i, chunk in enumerate(chunks)
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Power flow chunks"):
            chunk_results = future.result()
            all_results.extend(chunk_results)

    # Sort results by time index and convert to DataFrame
    all_results.sort(key=lambda x: x["time_idx"])
    df_results = pd.DataFrame(all_results)

    # Check convergence
    failed = df_results[~df_results["converged"]]
    if len(failed) > 0:
        logger.warning(f"{len(failed)} time steps failed to converge ({100 * len(failed) / steps:.2f}%)")

    logger.info("Simulation complete. Processing results...")

    # 5. Extract results for plotting
    dc_bus_vm = df_results["vm_pu"]
    total_slack = df_results["slack_p_mw"]
    dc_lines_loading = df_results["max_line_loading"]

    # Save raw results
    df_results.to_csv(output_dir / "pf_results.csv", index=False)

    # 6. Plotting
    fig, axes = plt.subplots(3, 1, figsize=(12, 12), sharex=True)

    # Plot 1: AI Load vs Slack Response
    axes[0].plot(df_profile["ai_load_mw"].values, label="AI Load (MW)", color="orange", alpha=0.8)
    axes[0].plot(total_slack.values, label="Slack Gen (MW)", color="blue", linestyle="--", alpha=0.6)
    axes[0].set_ylabel("Power (MW)")
    axes[0].set_title(f"System Response to {peak_load_mw} MW AI Cluster")
    axes[0].legend()
    axes[0].grid(True)

    # Plot 2: Voltage at Connection Point
    axes[1].plot(dc_bus_vm.values, label=f"Bus {grid_bus_idx} Voltage", color="red")
    axes[1].axhline(0.95, color="black", linestyle=":", label="Min Limit (0.95)")
    axes[1].set_ylabel("Voltage (p.u.)")
    axes[1].set_title("Voltage Stability")
    axes[1].legend()
    axes[1].grid(True)

    # Plot 3: Max Line Loading at DC connection
    axes[2].plot(dc_lines_loading.values, label="Max Line Loading", color="green")
    axes[2].axhline(100, color="black", linestyle=":", label="Thermal Limit (100%)")
    axes[2].set_ylabel("Loading (%)")
    axes[2].set_title("Local Transmission Line Loading (Max of Connected Lines)")
    axes[2].set_xlabel("Time Step (Minutes)")
    axes[2].legend()
    axes[2].grid(True)

    plt.tight_layout()
    plt.savefig(output_dir / "transmission_impact_summary.png", dpi=300)
    plt.close()

    # 7. Stats
    stats = {
        "min_voltage_pu": float(dc_bus_vm.min()),
        "max_line_loading_pct": float(dc_lines_loading.max()),
        "max_slack_ramp_mw": float(total_slack.diff().abs().max()),
        "peak_load_mw": peak_load_mw,
        "total_steps": steps,
        "failed_steps": int((~df_results["converged"]).sum()),
        "num_workers": num_workers,
    }

    with open(output_dir / "transmission_stats.json", "w") as f:
        json.dump(stats, f, indent=2)

    logger.info(f"Transmission study complete. Stats: {stats}")
