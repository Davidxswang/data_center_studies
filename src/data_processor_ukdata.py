import json
from pathlib import Path
from typing import Any
from enum import StrEnum
from datetime import datetime

import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field
from tqdm.rich import tqdm


class VoltageLevel(StrEnum):
    LOW = "Low Voltage Import"
    HIGH = "High Voltage Import"
    EXTRA_HIGH = "Extra-High Voltage Import"


class DCType(StrEnum):
    CO_LOCATED = "Co-located"
    ENTERPRISE = "Enterprise"


class DataCenterRecord(BaseModel):
    # anonymised_data_centre_name
    # cleansed_voltage_level
    # dc_type
    # ,,,local_timestamp,utc_timestamp,hh_utilisation_ratio
    id: int = Field(description="The ID of the data center")
    voltage_level: VoltageLevel
    dc_type: DCType
    utilizations: list[float]
    local_timestamps: list[datetime]
    utc_timestamps: list[datetime]

    def model_post_init(self, context: Any) -> None:
        if not (len(self.utilizations) == len(self.local_timestamps) == len(self.utc_timestamps)):
            raise AssertionError(f"Length mismatch: {len(self.utilizations)=}, {len(self.local_timestamps)=}, {len(self.utc_timestamps)=}")
        # utc timestamps should be monotonically increasing
        if not all(self.utc_timestamps[i] < self.utc_timestamps[i + 1] for i in range(len(self.utc_timestamps) - 1)):
            raise AssertionError(f"UTC timestamps are not monotonically increasing: {self.utc_timestamps=}")


class UKDataCenterData(BaseModel):
    records: list[DataCenterRecord]


def process_ukdata(input_data_path: Path) -> UKDataCenterData:
    """
    Process UK data centre demand profiles CSV file.

    Args:
        input_data_path: Path to the ukpn-data-centre-demand-profiles.csv file

    Returns:
        UKDataCentreData: Structured data containing all records
    """
    logger.info(f"Reading CSV from {input_data_path}")
    df = pd.read_csv(input_data_path)

    records: list[DataCenterRecord] = []
    unique_data_centers = df["anonymised_data_centre_name"].unique().tolist()
    for name in tqdm(unique_data_centers):
        assert name.startswith("Data Centre #")
        datacenter_id = int(name.replace("Data Centre #", ""))
        current_data_center_data = df[df["anonymised_data_centre_name"] == name]
        assert current_data_center_data["cleansed_voltage_level"].nunique() == 1
        voltage_level = VoltageLevel(current_data_center_data["cleansed_voltage_level"].iloc[0])
        assert current_data_center_data["dc_type"].nunique() == 1
        dc_type = DCType(current_data_center_data["dc_type"].iloc[0])
        current_data_center_data = current_data_center_data.copy().sort_values(by="utc_timestamp").reset_index(drop=True)
        utilizations = current_data_center_data["hh_utilisation_ratio"].tolist()
        local_timestamps = [datetime.fromisoformat(timestamp) for timestamp in current_data_center_data["local_timestamp"].tolist()]
        utc_timestamps = [datetime.fromisoformat(timestamp) for timestamp in current_data_center_data["utc_timestamp"].tolist()]
        records.append(
            DataCenterRecord(
                id=datacenter_id,
                voltage_level=voltage_level,
                dc_type=dc_type,
                utilizations=utilizations,
                local_timestamps=local_timestamps,
                utc_timestamps=utc_timestamps,
            )
        )

    return UKDataCenterData(records=records)


def visualize_ukdata(data: UKDataCenterData) -> str:
    """
    Generate an interactive HTML visualization of UK data center utilization data.

    Args:
        data: UKDataCenterData containing all data center records

    Returns:
        str: The HTML content
    """
    # Prepare data for JavaScript
    js_data = []
    for record in data.records:
        js_data.append(
            {
                "id": record.id,
                "voltage_level": record.voltage_level.value,
                "dc_type": record.dc_type.value,
                "utilizations": record.utilizations,
                "timestamps": [ts.isoformat() for ts in record.utc_timestamps],
            }
        )

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>UK Data Center Utilization Visualization</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            min-height: 100vh;
            color: #e0e0e0;
            padding: 20px;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        h1 {{
            text-align: center;
            margin-bottom: 30px;
            color: #00d4ff;
            font-size: 2rem;
            text-shadow: 0 0 10px rgba(0, 212, 255, 0.3);
        }}
        .controls {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
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
            color: #00d4ff;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        select {{
            padding: 12px;
            border: 1px solid rgba(0, 212, 255, 0.3);
            border-radius: 8px;
            background: rgba(255, 255, 255, 0.1);
            color: #e0e0e0;
            font-size: 1rem;
            cursor: pointer;
            transition: all 0.3s ease;
        }}
        select:hover, select:focus {{
            border-color: #00d4ff;
            outline: none;
            box-shadow: 0 0 15px rgba(0, 212, 255, 0.2);
        }}
        select[multiple] {{
            min-height: 150px;
        }}
        select option {{
            padding: 8px;
            background: #1a1a2e;
        }}
        select option:checked {{
            background: linear-gradient(0deg, #00d4ff 0%, #0099cc 100%);
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
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: rgba(255, 255, 255, 0.05);
            border-radius: 8px;
            padding: 15px;
            text-align: center;
            border: 1px solid rgba(0, 212, 255, 0.2);
        }}
        .stat-value {{
            font-size: 1.8rem;
            font-weight: bold;
            color: #00d4ff;
        }}
        .stat-label {{
            font-size: 0.85rem;
            color: #888;
            margin-top: 5px;
        }}
        .help-text {{
            font-size: 0.8rem;
            color: #888;
            margin-top: 4px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>UK Data Center Utilization Dashboard</h1>

        <div class="stats">
            <div class="stat-card">
                <div class="stat-value" id="total-dcs">{len(data.records)}</div>
                <div class="stat-label">Total Data Centers</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="selected-count">0</div>
                <div class="stat-label">Selected Data Centers</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="avg-utilization">-</div>
                <div class="stat-label">Avg Utilization (Selected)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="data-points">-</div>
                <div class="stat-label">Data Points</div>
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
                <label for="voltage-filter">Voltage Level</label>
                <select id="voltage-filter">
                    <option value="all">All Voltage Levels</option>
                    <option value="Low Voltage Import">Low Voltage</option>
                    <option value="High Voltage Import">High Voltage</option>
                    <option value="Extra-High Voltage Import">Extra-High Voltage</option>
                </select>
            </div>
            <div class="control-group">
                <label for="type-filter">Data Center Type</label>
                <select id="type-filter">
                    <option value="all">All Types</option>
                    <option value="Co-located">Co-located</option>
                    <option value="Enterprise">Enterprise</option>
                </select>
            </div>
        </div>

        <div class="chart-container">
            <div id="utilization-chart" style="height: 500px;"></div>
        </div>

        <div class="chart-container">
            <div id="distribution-chart" style="height: 400px;"></div>
        </div>
    </div>

    <script>
        const data = {json.dumps(js_data)};

        // Populate data center dropdown
        const dcSelect = document.getElementById('dc-select');
        const voltageFilter = document.getElementById('voltage-filter');
        const typeFilter = document.getElementById('type-filter');

        function populateDCDropdown() {{
            const voltageValue = voltageFilter.value;
            const typeValue = typeFilter.value;

            // Remember current selections
            const currentSelections = Array.from(dcSelect.selectedOptions).map(o => parseInt(o.value));

            dcSelect.innerHTML = '';

            const filteredData = data.filter(dc => {{
                if (voltageValue !== 'all' && dc.voltage_level !== voltageValue) return false;
                if (typeValue !== 'all' && dc.dc_type !== typeValue) return false;
                return true;
            }});

            filteredData.sort((a, b) => a.id - b.id).forEach(dc => {{
                const option = document.createElement('option');
                option.value = dc.id;
                option.textContent = `DC #${{dc.id}} (${{dc.voltage_level.replace(' Import', '')}}, ${{dc.dc_type}})`;
                if (currentSelections.includes(dc.id)) {{
                    option.selected = true;
                }}
                dcSelect.appendChild(option);
            }});

            updateCharts();
        }}

        function getSelectedData() {{
            const selectedIds = Array.from(dcSelect.selectedOptions).map(o => parseInt(o.value));
            return data.filter(dc => selectedIds.includes(dc.id));
        }}

        function updateStats(selectedData) {{
            document.getElementById('selected-count').textContent = selectedData.length;

            if (selectedData.length > 0) {{
                const allUtils = selectedData.flatMap(dc => dc.utilizations);
                const avgUtil = (allUtils.reduce((a, b) => a + b, 0) / allUtils.length * 100).toFixed(1);
                document.getElementById('avg-utilization').textContent = avgUtil + '%';
                document.getElementById('data-points').textContent = allUtils.length.toLocaleString();
            }} else {{
                document.getElementById('avg-utilization').textContent = '-';
                document.getElementById('data-points').textContent = '-';
            }}
        }}

        function updateCharts() {{
            const selectedData = getSelectedData();
            updateStats(selectedData);

            // Time series chart
            const traces = selectedData.map(dc => ({{
                x: dc.timestamps.map(t => new Date(t)),
                y: dc.utilizations.map(u => u * 100),
                type: 'scatter',
                mode: 'lines',
                name: `DC #${{dc.id}}`,
                line: {{ width: 1.5 }},
                hovertemplate: `DC #${{dc.id}}<br>%{{x}}<br>Utilization: %{{y:.1f}}%<extra></extra>`
            }}));

            const timeLayout = {{
                title: {{
                    text: 'Utilization Over Time',
                    font: {{ color: '#00d4ff', size: 18 }}
                }},
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                font: {{ color: '#e0e0e0' }},
                xaxis: {{
                    title: 'Time (UTC)',
                    gridcolor: 'rgba(255,255,255,0.1)',
                    linecolor: 'rgba(255,255,255,0.2)'
                }},
                yaxis: {{
                    title: 'Utilization (%)',
                    gridcolor: 'rgba(255,255,255,0.1)',
                    linecolor: 'rgba(255,255,255,0.2)',
                    range: [0, 100]
                }},
                legend: {{
                    bgcolor: 'rgba(0,0,0,0)',
                    font: {{ size: 10 }}
                }},
                hovermode: 'x unified'
            }};

            Plotly.newPlot('utilization-chart', traces.length > 0 ? traces : [{{
                x: [],
                y: [],
                type: 'scatter',
                mode: 'lines'
            }}], timeLayout, {{ responsive: true }});

            // Distribution chart (box plot)
            const boxTraces = selectedData.map(dc => ({{
                y: dc.utilizations.map(u => u * 100),
                type: 'box',
                name: `DC #${{dc.id}}`,
                boxmean: true
            }}));

            const distLayout = {{
                title: {{
                    text: 'Utilization Distribution by Data Center',
                    font: {{ color: '#00d4ff', size: 18 }}
                }},
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                font: {{ color: '#e0e0e0' }},
                xaxis: {{
                    gridcolor: 'rgba(255,255,255,0.1)',
                    linecolor: 'rgba(255,255,255,0.2)'
                }},
                yaxis: {{
                    title: 'Utilization (%)',
                    gridcolor: 'rgba(255,255,255,0.1)',
                    linecolor: 'rgba(255,255,255,0.2)',
                    range: [0, 100]
                }},
                showlegend: false
            }};

            Plotly.newPlot('distribution-chart', boxTraces.length > 0 ? boxTraces : [{{
                y: [],
                type: 'box'
            }}], distLayout, {{ responsive: true }});
        }}

        // Event listeners
        dcSelect.addEventListener('change', updateCharts);
        voltageFilter.addEventListener('change', populateDCDropdown);
        typeFilter.addEventListener('change', populateDCDropdown);

        // Initialize
        populateDCDropdown();

        // Select first 3 data centers by default for demo
        if (dcSelect.options.length > 0) {{
            for (let i = 0; i < Math.min(3, dcSelect.options.length); i++) {{
                dcSelect.options[i].selected = true;
            }}
            updateCharts();
        }}
    </script>
</body>
</html>
"""
    return html_content
