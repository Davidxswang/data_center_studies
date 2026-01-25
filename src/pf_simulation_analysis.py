import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandapower as pp  # type: ignore[import-untyped]
import pandapower.networks as nw  # type: ignore[import-untyped]
from pandapower import pandapowerNet
import pandas as pd
from loguru import logger
from tqdm.rich import tqdm

from src.data_processor_ukdata import UKDataCenterData, DataCenterRecord


def run_pf_simulation(
    dc_data: DataCenterRecord,
    output_dir: Path,
    network_name: str = "case33bw",
    node_idx: int = 18,
    dc_peak_mw: float = 2.0,
    hours: int = 24,
) -> None:
    """
    Runs a Time-Series Power Flow simulation.

    Args:
        dc_data: The specific data center profile to test.
        output_dir: The directory to save the results.
        network_name: The name of the network to use.
        node_idx: The index of the node to attach the DC (Data Center) load.
        dc_peak_mw: The assumed peak power capacity of the data center (MW).
        hours: Number of hours (steps) to run.
    """
    output_path = output_dir / f"pf_{network_name}_{node_idx}_{dc_peak_mw}_{hours}_{dc_data.id}.csv"
    if output_path.is_file():
        logger.info(f"Skipping simulation for {output_path} because it already exists")
        return None
    net: pandapowerNet = getattr(nw, network_name)()

    # 1. Add the Data Center Load element
    # We add it as a static load.
    # scaling: will be updated in the loop.
    dc_load_idx = pp.create_load(net, bus=node_idx, p_mw=0, q_mvar=0, name="Data Center")

    # We will simulate the first N time steps available in the profile
    # The profile has 30-min resolution
    steps = min(len(dc_data.utilizations), hours * 2)

    results: dict[str, list[Any]] = {
        "time": [],
        "voltage_dc_bus": [],  # pu
        "total_losses_mw": [],
        "dc_p_mw": [],
        "baseline_voltage": [],
        "baseline_losses": [],
    }

    # Run Baseline (No DC) first for comparison
    # We just set P_dc = 0
    net.load.at[dc_load_idx, "p_mw"] = 0
    net.load.at[dc_load_idx, "q_mvar"] = 0
    pp.runpp(net)
    v_base = net.res_bus.at[node_idx, "vm_pu"]
    loss_base = net.res_line["pl_mw"].sum()

    for i in tqdm(range(steps), desc=f"PP for DC {dc_data.id}"):
        # 1. Get Data Center Load for this timestamp
        util = dc_data.utilizations[i]
        p_mw = dc_peak_mw * util
        # Assume PF = 0.98 lagging -> Q = P * tan(acos(0.98)) ~= P * 0.203
        q_mvar = p_mw * 0.203

        # 2. Update Grid
        net.load.at[dc_load_idx, "p_mw"] = p_mw
        net.load.at[dc_load_idx, "q_mvar"] = q_mvar

        # 3. Run Power Flow
        try:
            pp.runpp(net)
        except pp.LoadflowNotConverged:
            logger.warning(f"Power flow did not converge at step {i} for Data Center {dc_data.id}")
        else:
            # 4. Record Results
            results["time"].append(dc_data.utc_timestamps[i])
            results["voltage_dc_bus"].append(net.res_bus.at[node_idx, "vm_pu"])
            results["total_losses_mw"].append(net.res_line["pl_mw"].sum())
            results["dc_p_mw"].append(p_mw)
            # Store constant baseline for easy plotting comparison
            results["baseline_voltage"].append(v_base)
            results["baseline_losses"].append(loss_base)

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path)


def plot_grid_impact_results(
    output_dir: Path,
    dc_ids: list[int],
    network_name: str,
    node_idx: int,
    peak_mw: float,
    hours: int,
) -> str:
    """
    Generate an interactive HTML visualization of grid impact analysis results.

    Args:
        output_dir: Directory containing the CSV result files.
        dc_ids: List of data center IDs to include in the plot.
        network_name: Name of the pandapower test network used.
        node_idx: Bus index where data center load was connected.
        peak_mw: Peak power of data center in MW.
        hours: Number of hours simulated.

    Returns:
        str: The HTML content.
    """
    # Load all results
    all_results: dict[int, pd.DataFrame] = {}
    for dc_id in dc_ids:
        csv_path = output_dir / f"pf_{network_name}_{node_idx}_{peak_mw}_{hours}_{dc_id}.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path, index_col=0, parse_dates=["time"])
            all_results[dc_id] = df

    if not all_results:
        logger.warning("No result files found to plot.")
        return "<html><body><h1>No results found</h1></body></html>"

    # Prepare data for JavaScript
    js_data = []
    for dc_id, df in all_results.items():
        js_data.append(
            {
                "id": dc_id,
                "time": df["time"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
                "voltage_dc_bus": df["voltage_dc_bus"].tolist(),
                "total_losses_mw": df["total_losses_mw"].tolist(),
                "dc_p_mw": df["dc_p_mw"].tolist(),
                "baseline_voltage": df["baseline_voltage"].tolist(),
                "baseline_losses": df["baseline_losses"].tolist(),
            }
        )

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Grid Impact Analysis Results</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
            min-height: 100vh;
            color: #e0e0e0;
            padding: 20px;
        }}
        .container {{
            max-width: 1600px;
            margin: 0 auto;
        }}
        h1 {{
            text-align: center;
            margin-bottom: 10px;
            color: #ffd700;
            font-size: 2rem;
            text-shadow: 0 0 10px rgba(255, 215, 0, 0.3);
        }}
        .subtitle {{
            text-align: center;
            color: #888;
            margin-bottom: 30px;
            font-size: 0.95rem;
        }}
        .controls {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
            padding: 20px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            backdrop-filter: blur(10px);
        }}
        .control-group {{
            display: flex;
            flex-direction: column;
            gap: 8px;
        }}
        label {{
            font-weight: 600;
            color: #ffd700;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        select {{
            padding: 12px;
            border: 1px solid rgba(255, 215, 0, 0.3);
            border-radius: 8px;
            background: rgba(255, 255, 255, 0.1);
            color: #e0e0e0;
            font-size: 1rem;
            cursor: pointer;
            transition: all 0.3s ease;
        }}
        select:hover, select:focus {{
            border-color: #ffd700;
            outline: none;
            box-shadow: 0 0 15px rgba(255, 215, 0, 0.2);
        }}
        select[multiple] {{
            min-height: 120px;
        }}
        select option {{
            padding: 8px;
            background: #1a1a2e;
        }}
        select option:checked {{
            background: linear-gradient(0deg, #ffd700 0%, #ffaa00 100%);
            color: #1a1a2e;
        }}
        .chart-container {{
            background: rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            backdrop-filter: blur(10px);
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: rgba(255, 255, 255, 0.05);
            border-radius: 8px;
            padding: 15px;
            text-align: center;
            border: 1px solid rgba(255, 215, 0, 0.2);
        }}
        .stat-value {{
            font-size: 1.5rem;
            font-weight: bold;
            color: #ffd700;
        }}
        .stat-value.warning {{
            color: #ff6b6b;
        }}
        .stat-value.good {{
            color: #51cf66;
        }}
        .stat-label {{
            font-size: 0.8rem;
            color: #888;
            margin-top: 5px;
        }}
        .help-text {{
            font-size: 0.8rem;
            color: #888;
            margin-top: 4px;
        }}
        .chart-row {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        @media (max-width: 1200px) {{
            .chart-row {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Grid Impact Analysis Dashboard</h1>
        <p class="subtitle">Power Flow Simulation Results - IEEE 33-Bus System</p>

        <div class="stats">
            <div class="stat-card">
                <div class="stat-value" id="total-dcs">{len(all_results)}</div>
                <div class="stat-label">Data Centers Analyzed</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="min-voltage">-</div>
                <div class="stat-label">Min Voltage (p.u.)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="max-voltage-drop">-</div>
                <div class="stat-label">Max Voltage Drop (%)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="max-losses">-</div>
                <div class="stat-label">Max Line Losses (MW)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="avg-dc-power">-</div>
                <div class="stat-label">Avg DC Power (MW)</div>
            </div>
        </div>

        <div class="controls">
            <div class="control-group">
                <label for="dc-select">Data Center IDs (Multi-Select)</label>
                <select id="dc-select" multiple>
                </select>
                <div class="help-text">Hold Ctrl/Cmd to select multiple</div>
            </div>
            <div class="control-group">
                <label for="show-baseline">Show Baseline</label>
                <select id="show-baseline">
                    <option value="yes">Yes - Show baseline comparison</option>
                    <option value="no">No - Only show with DC load</option>
                </select>
            </div>
        </div>

        <div class="chart-container">
            <div id="voltage-chart" style="height: 450px;"></div>
        </div>

        <div class="chart-row">
            <div class="chart-container">
                <div id="losses-chart" style="height: 400px;"></div>
            </div>
            <div class="chart-container">
                <div id="power-chart" style="height: 400px;"></div>
            </div>
        </div>

        <div class="chart-container">
            <div id="correlation-chart" style="height: 400px;"></div>
        </div>
    </div>

    <script>
        const data = {json.dumps(js_data)};

        const dcSelect = document.getElementById('dc-select');
        const showBaseline = document.getElementById('show-baseline');

        // Populate dropdown
        data.sort((a, b) => a.id - b.id).forEach(dc => {{
            const option = document.createElement('option');
            option.value = dc.id;
            option.textContent = `Data Center #${{dc.id}}`;
            dcSelect.appendChild(option);
        }});

        // Select first 3 by default
        for (let i = 0; i < Math.min(3, dcSelect.options.length); i++) {{
            dcSelect.options[i].selected = true;
        }}

        function getSelectedData() {{
            const selectedIds = Array.from(dcSelect.selectedOptions).map(o => parseInt(o.value));
            return data.filter(dc => selectedIds.includes(dc.id));
        }}

        function updateStats(selectedData) {{
            if (selectedData.length === 0) {{
                document.getElementById('min-voltage').textContent = '-';
                document.getElementById('max-voltage-drop').textContent = '-';
                document.getElementById('max-losses').textContent = '-';
                document.getElementById('avg-dc-power').textContent = '-';
                return;
            }}

            const allVoltages = selectedData.flatMap(dc => dc.voltage_dc_bus);
            const allBaseline = selectedData.flatMap(dc => dc.baseline_voltage);
            const allLosses = selectedData.flatMap(dc => dc.total_losses_mw);
            const allPower = selectedData.flatMap(dc => dc.dc_p_mw);

            const minV = Math.min(...allVoltages);
            const baseV = allBaseline[0] || 1.0;
            const maxDrop = ((baseV - minV) / baseV * 100);
            const maxLoss = Math.max(...allLosses);
            const avgPower = allPower.reduce((a, b) => a + b, 0) / allPower.length;

            const minVEl = document.getElementById('min-voltage');
            minVEl.textContent = minV.toFixed(4);
            minVEl.className = 'stat-value' + (minV < 0.95 ? ' warning' : ' good');

            const maxDropEl = document.getElementById('max-voltage-drop');
            maxDropEl.textContent = maxDrop.toFixed(2) + '%';
            maxDropEl.className = 'stat-value' + (maxDrop > 5 ? ' warning' : ' good');

            document.getElementById('max-losses').textContent = maxLoss.toFixed(3);
            document.getElementById('avg-dc-power').textContent = avgPower.toFixed(2);
        }}

        function updateCharts() {{
            const selectedData = getSelectedData();
            const showBase = showBaseline.value === 'yes';
            updateStats(selectedData);

            // Voltage Time Series
            const voltageTraces = [];
            selectedData.forEach(dc => {{
                voltageTraces.push({{
                    x: dc.time.map(t => new Date(t)),
                    y: dc.voltage_dc_bus,
                    type: 'scatter',
                    mode: 'lines',
                    name: `DC #${{dc.id}}`,
                    line: {{ width: 1.5 }},
                    hovertemplate: `DC #${{dc.id}}<br>%{{x}}<br>Voltage: %{{y:.4f}} p.u.<extra></extra>`
                }});
            }});

            if (showBase && selectedData.length > 0) {{
                voltageTraces.push({{
                    x: selectedData[0].time.map(t => new Date(t)),
                    y: selectedData[0].baseline_voltage,
                    type: 'scatter',
                    mode: 'lines',
                    name: 'Baseline (No DC)',
                    line: {{ width: 2, dash: 'dash', color: '#888' }},
                }});
            }}

            Plotly.newPlot('voltage-chart', voltageTraces, {{
                title: {{ text: 'Bus Voltage Over Time', font: {{ color: '#ffd700', size: 16 }} }},
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                font: {{ color: '#e0e0e0' }},
                xaxis: {{ title: 'Time (UTC)', gridcolor: 'rgba(255,255,255,0.1)' }},
                yaxis: {{ title: 'Voltage (p.u.)', gridcolor: 'rgba(255,255,255,0.1)' }},
                legend: {{ bgcolor: 'rgba(0,0,0,0)' }},
                hovermode: 'x unified',
                shapes: [{{
                    type: 'line', y0: 0.95, y1: 0.95, x0: 0, x1: 1, xref: 'paper',
                    line: {{ color: '#ff6b6b', width: 1, dash: 'dot' }}
                }}]
            }}, {{ responsive: true }});

            // Losses Chart
            const lossTraces = [];
            selectedData.forEach(dc => {{
                lossTraces.push({{
                    x: dc.time.map(t => new Date(t)),
                    y: dc.total_losses_mw,
                    type: 'scatter',
                    mode: 'lines',
                    name: `DC #${{dc.id}}`,
                    fill: 'tozeroy',
                    line: {{ width: 1 }},
                }});
            }});

            if (showBase && selectedData.length > 0) {{
                lossTraces.push({{
                    x: selectedData[0].time.map(t => new Date(t)),
                    y: selectedData[0].baseline_losses,
                    type: 'scatter',
                    mode: 'lines',
                    name: 'Baseline',
                    line: {{ width: 2, dash: 'dash', color: '#888' }},
                }});
            }}

            Plotly.newPlot('losses-chart', lossTraces, {{
                title: {{ text: 'Total Line Losses', font: {{ color: '#ffd700', size: 16 }} }},
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                font: {{ color: '#e0e0e0' }},
                xaxis: {{ title: 'Time', gridcolor: 'rgba(255,255,255,0.1)' }},
                yaxis: {{ title: 'Losses (MW)', gridcolor: 'rgba(255,255,255,0.1)' }},
                legend: {{ bgcolor: 'rgba(0,0,0,0)', font: {{ size: 10 }} }},
                showlegend: true
            }}, {{ responsive: true }});

            // DC Power Chart
            const powerTraces = selectedData.map(dc => ({{
                x: dc.time.map(t => new Date(t)),
                y: dc.dc_p_mw,
                type: 'scatter',
                mode: 'lines',
                name: `DC #${{dc.id}}`,
                line: {{ width: 1.5 }},
            }}));

            Plotly.newPlot('power-chart', powerTraces, {{
                title: {{ text: 'Data Center Power Consumption', font: {{ color: '#ffd700', size: 16 }} }},
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                font: {{ color: '#e0e0e0' }},
                xaxis: {{ title: 'Time', gridcolor: 'rgba(255,255,255,0.1)' }},
                yaxis: {{ title: 'Power (MW)', gridcolor: 'rgba(255,255,255,0.1)' }},
                legend: {{ bgcolor: 'rgba(0,0,0,0)', font: {{ size: 10 }} }},
            }}, {{ responsive: true }});

            // Correlation: Power vs Voltage Drop
            const corrTraces = selectedData.map(dc => {{
                const baseV = dc.baseline_voltage[0] || 1.0;
                return {{
                    x: dc.dc_p_mw,
                    y: dc.voltage_dc_bus.map(v => (baseV - v) * 100),
                    type: 'scatter',
                    mode: 'markers',
                    name: `DC #${{dc.id}}`,
                    marker: {{ size: 4, opacity: 0.6 }},
                    hovertemplate: `DC #${{dc.id}}<br>Power: %{{x:.2f}} MW<br>Voltage Drop: %{{y:.2f}}%<extra></extra>`
                }};
            }});

            Plotly.newPlot('correlation-chart', corrTraces, {{
                title: {{ text: 'Power vs Voltage Drop Correlation', font: {{ color: '#ffd700', size: 16 }} }},
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                font: {{ color: '#e0e0e0' }},
                xaxis: {{ title: 'DC Power (MW)', gridcolor: 'rgba(255,255,255,0.1)' }},
                yaxis: {{ title: 'Voltage Drop (%)', gridcolor: 'rgba(255,255,255,0.1)' }},
                legend: {{ bgcolor: 'rgba(0,0,0,0)' }},
            }}, {{ responsive: true }});
        }}

        dcSelect.addEventListener('change', updateCharts);
        showBaseline.addEventListener('change', updateCharts);

        updateCharts();
    </script>
</body>
</html>
"""
    return html_content


def analyze_grid_impact_results(
    output_dir: Path,
    dc_ids: list[int],
    network_name: str,
    node_idx: int,
    peak_mw: float,
    hours: int,
) -> dict[str, Any]:
    """
    Analyze grid impact results and generate summary statistics.

    Args:
        output_dir: Directory containing the CSV result files.
        dc_ids: List of data center IDs to analyze.
        network_name: Name of the pandapower test network used.
        node_idx: Bus index where data center load was connected.
        peak_mw: Peak power of data center in MW.
        hours: Number of hours simulated.

    Returns:
        dict: Analysis results including statistics and recommendations.
    """
    analysis: dict[str, Any] = {
        "summary": {},
        "per_dc_stats": {},
        "voltage_violations": [],
        "recommendations": [],
    }

    all_dfs: dict[int, pd.DataFrame] = {}
    for dc_id in dc_ids:
        csv_path = output_dir / f"pf_{network_name}_{node_idx}_{peak_mw}_{hours}_{dc_id}.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path, index_col=0, parse_dates=["time"])
            all_dfs[dc_id] = df

    if not all_dfs:
        logger.warning("No result files found to analyze.")
        return analysis

    # Per-DC statistics
    for dc_id, df in all_dfs.items():
        baseline_v = df["baseline_voltage"].iloc[0]
        voltage_drop_pct = (baseline_v - df["voltage_dc_bus"]) / baseline_v * 100

        stats = {
            "min_voltage_pu": float(df["voltage_dc_bus"].min()),
            "max_voltage_pu": float(df["voltage_dc_bus"].max()),
            "mean_voltage_pu": float(df["voltage_dc_bus"].mean()),
            "max_voltage_drop_pct": float(voltage_drop_pct.max()),
            "mean_voltage_drop_pct": float(voltage_drop_pct.mean()),
            "min_losses_mw": float(df["total_losses_mw"].min()),
            "max_losses_mw": float(df["total_losses_mw"].max()),
            "mean_losses_mw": float(df["total_losses_mw"].mean()),
            "baseline_losses_mw": float(df["baseline_losses"].iloc[0]),
            "max_additional_losses_mw": float(df["total_losses_mw"].max() - df["baseline_losses"].iloc[0]),
            "mean_dc_power_mw": float(df["dc_p_mw"].mean()),
            "max_dc_power_mw": float(df["dc_p_mw"].max()),
            "data_points": len(df),
        }
        analysis["per_dc_stats"][dc_id] = stats

        # Check for voltage violations (< 0.95 p.u. is typically the limit)
        violations = df[df["voltage_dc_bus"] < 0.95]
        if len(violations) > 0:
            analysis["voltage_violations"].append(
                {
                    "dc_id": dc_id,
                    "violation_count": len(violations),
                    "violation_percentage": float(len(violations) / len(df) * 100),
                    "worst_voltage": float(violations["voltage_dc_bus"].min()),
                    "worst_time": str(violations.loc[violations["voltage_dc_bus"].idxmin(), "time"]),
                }
            )

    # Overall summary
    all_voltages = pd.concat([df["voltage_dc_bus"] for df in all_dfs.values()])
    all_losses = pd.concat([df["total_losses_mw"] for df in all_dfs.values()])
    all_power = pd.concat([df["dc_p_mw"] for df in all_dfs.values()])

    analysis["summary"] = {
        "total_dcs_analyzed": len(all_dfs),
        "total_data_points": int(all_voltages.count()),
        "overall_min_voltage_pu": float(all_voltages.min()),
        "overall_max_voltage_drop_pct": float(
            (list(all_dfs.values())[0]["baseline_voltage"].iloc[0] - all_voltages.min()) / list(all_dfs.values())[0]["baseline_voltage"].iloc[0] * 100
        ),
        "overall_max_losses_mw": float(all_losses.max()),
        "overall_mean_losses_mw": float(all_losses.mean()),
        "overall_mean_dc_power_mw": float(all_power.mean()),
        "dcs_with_violations": len(analysis["voltage_violations"]),
    }

    # Generate recommendations
    if analysis["voltage_violations"]:
        analysis["recommendations"].append(
            "VOLTAGE ISSUE: Some data center profiles cause voltage to drop below 0.95 p.u. "
            "Consider reactive power compensation (capacitor banks) or network reinforcement."
        )

    max_loss_increase = max(stats["max_additional_losses_mw"] for stats in analysis["per_dc_stats"].values())
    if max_loss_increase > 0.1:
        analysis["recommendations"].append(
            f"LOSSES ISSUE: Maximum additional losses of {max_loss_increase:.3f} MW observed. Consider load balancing or distributed generation."
        )

    if not analysis["recommendations"]:
        analysis["recommendations"].append("Grid impact is within acceptable limits for all analyzed data center profiles.")

    return analysis


def run_pf_simulation_and_analysis(
    data_json_path: Path,
    output_dir: Path,
    network_name: str = "case33bw",
    node_idx: int = 18,
    peak_mw: float = 5.0,
    hours: int = 24 * 365,
) -> None:
    """
    Run complete grid impact analysis pipeline.

    Args:
        data_json_path: Path to the UK data JSON file.
        output_dir: Directory to save results.
        network_name: Name of the pandapower test network.
        node_idx: Bus index to connect data center load.
        peak_mw: Assumed peak power of data center in MW.
        hours: Number of hours to simulate.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load Data
    if not data_json_path.is_file():
        logger.error(f"Data file {data_json_path} not found. Please run 'uv run main.py preprocess_ukdata' first.")
        return
    with open(data_json_path, "r", encoding="utf-8") as f:
        full_data = UKDataCenterData.model_validate_json(f.read())
    if not full_data.records:
        logger.error("No records found in data.")
        return

    dc_ids = [record.id for record in full_data.records]

    # 2. Run Power Flow for all data centers assuming they are connected to certain bus for a certain network
    # Use multiprocessing to speed up (each simulation is independent)
    num_workers = min(len(full_data.records), os.cpu_count() or 4)
    logger.info(f"Running power flow simulations with {num_workers} parallel workers...")

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(
                run_pf_simulation,
                dc_data=record,
                output_dir=output_dir,
                network_name=network_name,
                node_idx=node_idx,
                dc_peak_mw=peak_mw,
                hours=hours,
            ): record.id
            for record in full_data.records
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Power flow simulations"):
            dc_id = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Simulation failed for DC #{dc_id}: {e}")

    # 3. Plot the results using plotly -> html
    logger.info("Generating visualization...")
    html_content = plot_grid_impact_results(output_dir, dc_ids, network_name, node_idx, peak_mw, hours)
    with open(output_dir / "grid_impact_visualization.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    logger.info(f"Visualization saved to {output_dir / 'grid_impact_visualization.html'}")

    # 4. Analyze the results
    logger.info("Analyzing results...")
    analysis = analyze_grid_impact_results(output_dir, dc_ids, network_name, node_idx, peak_mw, hours)

    # Save analysis as JSON
    with open(output_dir / "grid_impact_analysis.json", "w", encoding="utf-8") as f:
        json.dump(analysis, f, indent=2, default=str)
    logger.info(f"Analysis saved to {output_dir / 'grid_impact_analysis.json'}")

    # Print summary
    logger.info("=" * 60)
    logger.info("GRID IMPACT ANALYSIS SUMMARY")
    logger.info("=" * 60)
    summary = analysis["summary"]
    logger.info(f"Data Centers Analyzed: {summary['total_dcs_analyzed']}")
    logger.info(f"Total Data Points: {summary['total_data_points']}")
    logger.info(f"Overall Min Voltage: {summary['overall_min_voltage_pu']:.4f} p.u.")
    logger.info(f"Overall Max Voltage Drop: {summary['overall_max_voltage_drop_pct']:.2f}%")
    logger.info(f"Overall Max Line Losses: {summary['overall_max_losses_mw']:.4f} MW")
    logger.info(f"DCs with Voltage Violations: {summary['dcs_with_violations']}")
    logger.info("-" * 60)
    logger.info("RECOMMENDATIONS:")
    for rec in analysis["recommendations"]:
        logger.info(f"  - {rec}")
