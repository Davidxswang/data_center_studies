from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from loguru import logger

from src.data_processor_2026 import Spot2026


def analyze_volatility(
    data_path: Path,
    output_dir: Path,
    w_cpu: float = 4.0,  # Watts per core
    w_gpu_map: dict[str, float] | None = None,
) -> None:
    """
    Analyze the volatility of the 2026 spot trace aggregate power curve.

    Args:
        data_path: Path to the processed data.json file.
        output_dir: Directory to save plots and stats.
        w_cpu: Power consumption per CPU core (Watts).
        w_gpu_map: Dictionary mapping GPU model names to power consumption (Watts).
    """
    if w_gpu_map is None:
        # Defaults based on A100/H100 specs and dataset assumptions
        w_gpu_map = {
            "A800-SXM4-80GB": 350.0,
            "A100-SXM4-80GB": 400.0,
            "A10": 150.0,
            "GPU-series-1": 400.0,  # Assume A100-class
            "GPU-series-2": 700.0,  # Assume H100-class
            "H800": 400.0,  # H800 PCIe
        }

    logger.info(f"Loading data from {data_path}...")
    with open(data_path, "r", encoding="utf-8") as f:
        data = Spot2026.model_validate_json(f.read())

    jobs = data.jobs
    logger.info(f"Loaded {len(jobs)} jobs. Building power curve...")

    # 1. Build Time Series
    # We need to aggregate power at every second (or minute)
    # Event-based approach is most efficient:
    # event[t] = +power (job start), event[t+duration] = -power (job end)

    events: dict[int, float] = {}
    min_time = min(j.submit_time for j in jobs)
    max_time = max(j.submit_time + j.duration for j in jobs)

    for job in jobs:
        # Calculate Job Power
        p_cpu = job.cpu_num * w_cpu
        p_gpu = job.gpu_num * w_gpu_map[job.gpu_model]
        p_total = (p_cpu + p_gpu) * job.worker_num

        # Add events
        start = job.submit_time
        end = start + job.duration

        events[start] = events.get(start, 0.0) + p_total
        events[end] = events.get(end, 0.0) - p_total

    # 2. Sort and Integrate
    sorted_times = sorted(events.keys())
    current_power = 0.0
    times_list = []
    power_list = []

    # Sample at 1-minute resolution for analysis (avoids noise of seconds)
    # But first, let's just create the step function arrays
    for t in sorted_times:
        times_list.append(t)
        power_list.append(current_power)  # Step value before change
        current_power += events[t]
        times_list.append(t)
        power_list.append(current_power)  # Step value after change

    df = pd.DataFrame({"time": times_list, "power_w": power_list})
    df["power_mw"] = df["power_w"] / 1e6

    # 3. Calculate Volatility Metrics
    # We should resample to a regular grid (e.g., 1 minute) to calculating derivatives
    # Create a regular time index
    full_idx = np.arange(min_time, max_time + 1, 60)  # 1-minute steps
    # We need to "asof" merge or interpolate.
    # Since power is constant between events, 'ffill' is correct.
    df_resampled = pd.DataFrame({"time": full_idx})
    # Use searchsorted to find the power at each sampled time
    # This is faster than pandas merge_asof for simple step functions
    indices = np.searchsorted(df["time"], df_resampled["time"], side="right") - 1
    df_resampled["power_mw"] = df["power_mw"].iloc[indices].values

    # TRIM ARTIFACTS: Cut off the first 1% and last 1% of the timeline
    # This removes the "warm-up" and "cliff-edge" effects of the trace boundaries.
    total_duration = max_time - min_time
    buffer = total_duration * 0.01
    start_valid = min_time + buffer
    end_valid = max_time - buffer

    logger.info(f"Trimming trace tails (1% buffer). Analysis window: {start_valid:.0f}s to {end_valid:.0f}s")
    df_analysis = df_resampled[(df_resampled["time"] >= start_valid) & (df_resampled["time"] <= end_valid)].copy()

    # Metrics (on trimmed data)
    peak_mw = df_analysis["power_mw"].max()
    avg_mw = df_analysis["power_mw"].mean()
    min_mw = df_analysis["power_mw"].min()
    par = peak_mw / avg_mw if avg_mw > 0 else 0.0
    std_dev = df_analysis["power_mw"].std()

    # Ramp Rates (MW/min)
    df_analysis["ramp_mw"] = df_analysis["power_mw"].diff()
    max_ramp_up = df_analysis["ramp_mw"].max()
    max_ramp_down = df_analysis["ramp_mw"].min()

    stats = {
        "peak_mw": peak_mw,
        "avg_mw": avg_mw,
        "min_mw": min_mw,
        "par": par,
        "std_dev_mw": std_dev,
        "max_ramp_up_mw_per_min": max_ramp_up,
        "max_ramp_down_mw_per_min": max_ramp_down,
        "total_energy_mwh": (df_analysis["power_mw"].sum() * (1 / 60)),  # Sum(MW * 1 min) -> MWh
    }

    logger.info("Volatility Analysis Results:")
    for k, v in stats.items():
        logger.info(f"  {k}: {v:.4f}")

    # 4. Plotting
    output_dir.mkdir(parents=True, exist_ok=True)

    # Plot 1: Full Time Series (using the trimmed data for cleaner view)
    plt.figure(figsize=(12, 6))
    plt.plot(df_analysis["time"] / 3600, df_analysis["power_mw"], linewidth=0.5)
    plt.title("Aggregate AI Cluster Power Draw (2026 Trace - Trimmed)")
    plt.xlabel("Time (Hours)")
    plt.ylabel("Power (MW)")
    plt.grid(True, alpha=0.3)
    plt.savefig(output_dir / "power_trace_full.png", dpi=300)
    plt.close()

    # Plot 2: Histogram of Load
    plt.figure(figsize=(10, 6))
    plt.hist(df_analysis["power_mw"], bins=50, color="skyblue", edgecolor="black")
    plt.title("Distribution of Power Consumption")
    plt.xlabel("Power (MW)")
    plt.ylabel("Frequency (Minutes)")
    plt.grid(True, alpha=0.3)
    plt.savefig(output_dir / "power_histogram.png", dpi=300)
    plt.close()

    # Plot 3: Zoom into a high-volatility window
    # Find the day with the max ramp
    ramp_idx = df_analysis["ramp_mw"].abs().idxmax()
    # ramp_idx is an index label from the sliced dataframe, we need integer location for slicing window
    # easiest is to get the time of the ramp
    if pd.isna(ramp_idx):
        logger.warning("No ramp data found (NaN). Skipping zoom plot.")
    else:
        ramp_time = float(df_analysis.loc[ramp_idx, "time"])  # type: ignore[arg-type]
        window_sec = 24 * 3600  # 24 hours
        zoom_start = ramp_time - window_sec / 2
        zoom_end = ramp_time + window_sec / 2

        subset = df_analysis[(df_analysis["time"] >= zoom_start) & (df_analysis["time"] <= zoom_end)]

        plt.figure(figsize=(12, 6))
        plt.plot(subset["time"] / 3600, subset["power_mw"])
        plt.title("Zoomed View: High Volatility Event")
        plt.xlabel("Time (Hours)")
        plt.ylabel("Power (MW)")
        plt.grid(True, alpha=0.3)
        plt.savefig(output_dir / "power_trace_zoom.png", dpi=300)
        plt.close()

    # Save stats
    pd.Series(stats).to_csv(output_dir / "volatility_stats.csv")

    # Save the aggregated profile for later use (OPF)
    # We save the TRIMMED profile as the 'golden' one for simulation
    df_analysis[["time", "power_mw"]].to_csv(output_dir / "aggregated_load_profile.csv", index=False)
    logger.info(f"Analysis complete. Results saved to {output_dir}")

    return None
