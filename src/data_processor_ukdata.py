import json
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field
from tqdm.rich import tqdm


class DataCentreRecord(BaseModel):
    voltage_level: Literal["Low Voltage Import", "High Voltage Import", "Extra-High Voltage Import"]
    data_centre_name: str
    dc_type: Literal["Co-located", "Enterprise"]
    local_timestamp: str
    utc_timestamp: str
    utilisation_ratio: float = Field(ge=0.0)


class UKDataCentreData(BaseModel):
    records: list[DataCentreRecord]

    @property
    def unique_data_centres(self) -> set[str]:
        return {record.data_centre_name for record in self.records}

    @property
    def unique_voltage_levels(self) -> set[str]:
        return {record.voltage_level for record in self.records}

    @property
    def unique_dc_types(self) -> set[str]:
        return {record.dc_type for record in self.records}

    def model_post_init(self, context: Any) -> None:
        logger.info(f"Loaded {len(self.records)} records")
        logger.info(f"Unique data centres: {len(self.unique_data_centres)}")
        logger.info(f"Voltage levels: {sorted(self.unique_voltage_levels)}")
        logger.info(f"DC types: {sorted(self.unique_dc_types)}")


def process_ukdata(input_data_path: Path) -> UKDataCentreData:
    """
    Process UK data centre demand profiles CSV file.

    Args:
        input_data_path: Path to the ukpn-data-centre-demand-profiles.csv file

    Returns:
        UKDataCentreData: Structured data containing all records
    """
    logger.info(f"Reading CSV from {input_data_path}")
    df = pd.read_csv(input_data_path)

    records: list[DataCentreRecord] = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing UK data"):
        record = DataCentreRecord(
            voltage_level=row["cleansed_voltage_level"],
            data_centre_name=row["anonymised_data_centre_name"],
            dc_type=row["dc_type"],
            local_timestamp=row["local_timestamp"],
            utc_timestamp=row["utc_timestamp"],
            utilisation_ratio=float(row["hh_utilisation_ratio"]),
        )
        records.append(record)

    data = UKDataCentreData(records=records)
    logger.info(f"Processed {len(records)} records from UK data centre profiles")

    return data


def visualize_ukdata(data: UKDataCentreData) -> str:
    """
    Visualize the UK data centre demand profiles and return the HTML string.

    This function aggregates data by hour to reduce the output size significantly.
    With 4M+ records, we cannot embed all raw data in the HTML.

    Args:
        data: The data to visualize

    Returns:
        The HTML string with interactive Plotly visualization
    """
    records = data.records
    total_records = len(records)
    unique_dcs = sorted(data.unique_data_centres)
    unique_voltage_levels = sorted(data.unique_voltage_levels)
    unique_dc_types = sorted(data.unique_dc_types)

    logger.info(f"Processing {total_records} records for visualization (aggregating by hour)")

    # Convert to DataFrame for easier analysis
    df = pd.DataFrame(
        [
            {
                "data_centre": r.data_centre_name,
                "voltage_level": r.voltage_level,
                "dc_type": r.dc_type,
                "timestamp": r.utc_timestamp,
                "utilisation": r.utilisation_ratio,
            }
            for r in records
        ]
    )

    # Calculate time range
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"])
    min_time = df["timestamp_dt"].min().isoformat()
    max_time = df["timestamp_dt"].max().isoformat()

    # Calculate summary statistics
    avg_utilisation = df["utilisation"].mean()
    max_utilisation = df["utilisation"].max()
    min_utilisation = df["utilisation"].min()

    # Group by data centre for summary stats
    dc_stats = df.groupby("data_centre")["utilisation"].agg(["mean", "max", "min", "count"])

    # Get metadata for each DC (voltage level and type)
    dc_metadata = df.groupby("data_centre")[["voltage_level", "dc_type"]].first()

    # Aggregate data by hour to reduce size
    # This reduces ~4M records to a manageable size
    df["hour"] = df["timestamp_dt"].dt.floor("H")

    logger.info("Aggregating data by hour per data centre")
    dc_series: dict[str, dict[str, Any]] = {}

    for dc_name in tqdm(unique_dcs, desc="Processing data centres"):
        dc_data = df[df["data_centre"] == dc_name]

        # Aggregate by hour: take mean utilisation for each hour
        hourly_data = dc_data.groupby("hour")["utilisation"].mean().reset_index()
        hourly_data = hourly_data.sort_values("hour")

        dc_series[dc_name] = {
            "timestamps": hourly_data["hour"].dt.strftime("%Y-%m-%dT%H:%M:%SZ").tolist(),
            "utilisation": hourly_data["utilisation"].round(4).tolist(),
            "voltage_level": str(dc_metadata.loc[dc_name, "voltage_level"]) if dc_name in dc_metadata.index else "",
            "dc_type": str(dc_metadata.loc[dc_name, "dc_type"]) if dc_name in dc_metadata.index else "",
            "mean": float(dc_stats.loc[dc_name, "mean"]) if dc_name in dc_stats.index else 0,  # type: ignore[arg-type]
            "max": float(dc_stats.loc[dc_name, "max"]) if dc_name in dc_stats.index else 0,  # type: ignore[arg-type]
            "count": int(dc_stats.loc[dc_name, "count"]) if dc_name in dc_stats.index else 0,  # type: ignore[arg-type]
        }

    total_aggregated_points = sum(len(dc["timestamps"]) for dc in dc_series.values())
    logger.info(f"Aggregated {total_records} records to {total_aggregated_points} hourly data points")

    data_payload = {
        "total_records": total_records,
        "unique_dcs": unique_dcs,
        "unique_voltage_levels": unique_voltage_levels,
        "unique_dc_types": unique_dc_types,
        "min_time": min_time,
        "max_time": max_time,
        "avg_utilisation": float(avg_utilisation),
        "max_utilisation": float(max_utilisation),
        "min_utilisation": float(min_utilisation),
        "dc_series": dc_series,
    }

    # Create options for dropdowns
    dc_options = "".join(f'<option value="{name}">{name}</option>' for name in unique_dcs)

    html_str = f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8"/>
    <title>UKPN Data Centre Demand Profiles</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 24px; color: #1a1a1a; }}
      h1 {{ margin-bottom: 8px; }}
      .summary {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin: 16px 0; }}
      .card {{ padding: 12px 14px; border: 1px solid #e5e7eb; border-radius: 8px; }}
      .card h3 {{ margin: 0 0 6px 0; font-size: 14px; color: #555; }}
      .card p {{ margin: 0; font-size: 18px; font-weight: 600; }}
      .controls {{ display: grid; grid-template-columns: 1fr; gap: 12px; margin: 16px 0; align-items: end; max-width: 400px; }}
      .control {{ border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px; }}
      .control label {{ display: block; font-size: 12px; color: #555; margin-bottom: 6px; }}
      .control select {{ width: 100%; }}
      .note {{ margin-top: 8px; font-size: 12px; color: #666; }}
      #chart {{ width: 100%; height: 520px; }}
      .info {{ margin: 16px 0; padding: 12px 14px; border: 1px solid #e5e7eb; border-radius: 8px; }}
      .info h3 {{ margin: 0 0 8px 0; font-size: 14px; color: #555; }}
      .info p {{ margin: 6px 0; font-size: 13px; color: #333; }}
    </style>
  </head>
  <body>
    <h1>UKPN Data Centre Demand Profiles</h1>
    <div class="summary">
      <div class="card"><h3>Total Records</h3><p id="summaryRecords">-</p></div>
      <div class="card"><h3>Data Centres</h3><p id="summaryDCs">-</p></div>
      <div class="card"><h3>Time Range</h3><p id="summaryTimeRange" style="font-size: 14px;">-</p></div>
      <div class="card"><h3>Avg Utilisation</h3><p id="summaryAvgUtil">-</p></div>
    </div>

    <div class="controls">
      <div class="control">
        <label for="dcSelect">Data Centre</label>
        <select id="dcSelect">
          {dc_options}
        </select>
      </div>
    </div>

    <div class="info">
      <h3>Current Selection Info</h3>
      <p id="selectionInfo">Select a data centre to view its utilisation profile.</p>
    </div>

    <div id="chart"></div>
    <div class="note">
      <strong>About this data:</strong> UKPN (UK Power Networks) data centre demand profiles showing utilisation ratios (0-1)
      across different data centres categorized by voltage level and type (Co-located or Enterprise).<br>
      <strong>Data aggregation:</strong> Original half-hourly data (~{total_records:,} records) has been aggregated to hourly averages
      (~{total_aggregated_points:,} points) for efficient visualization.<br>
      Use Plotly interactions: drag to zoom, double-click to reset, hover for values.
    </div>

    <script>
      const DATA = {json.dumps(data_payload)};

      function updateSummary() {{
        document.getElementById('summaryRecords').textContent = DATA.total_records.toLocaleString();
        document.getElementById('summaryDCs').textContent = DATA.unique_dcs.length;
        const startDate = new Date(DATA.min_time).toLocaleDateString();
        const endDate = new Date(DATA.max_time).toLocaleDateString();
        document.getElementById('summaryTimeRange').textContent = `${{startDate}} - ${{endDate}}`;
        document.getElementById('summaryAvgUtil').textContent = (DATA.avg_utilisation * 100).toFixed(1) + '%';
      }}

      function render() {{
        const selectedDC = document.getElementById('dcSelect').value;
        const series = DATA.dc_series[selectedDC];

        if (!series) {{
          document.getElementById('selectionInfo').textContent = 'No data available for selected data centre.';
          return;
        }}

        const x = series.timestamps.map(ts => new Date(ts));
        const y = series.utilisation;

        const traces = [{{
          x: x,
          y: y,
          mode: 'lines',
          name: selectedDC,
          line: {{ width: 1.5, color: '#1f77b4' }},
          hovertemplate: 'Time: %{{x}}<br>Utilisation: %{{y:.2%}}<extra></extra>',
        }}];

        const infoText = `<strong>${{selectedDC}}</strong><br>` +
                         `Voltage Level: ${{series.voltage_level}}<br>` +
                         `DC Type: ${{series.dc_type}}<br>` +
                         `Avg Utilisation: ${{(series.mean * 100).toFixed(1)}}%<br>` +
                         `Max Utilisation: ${{(series.max * 100).toFixed(1)}}%<br>` +
                         `Data Points: ${{series.count.toLocaleString()}}`;

        document.getElementById('selectionInfo').innerHTML = infoText;

        const layout = {{
          margin: {{ l: 60, r: 20, t: 30, b: 50 }},
          xaxis: {{ title: 'Time', type: 'date' }},
          yaxis: {{
            title: 'Utilisation Ratio',
            tickformat: '.0%',
            range: [0, 1]
          }},
          hovermode: 'closest',
        }};

        Plotly.react('chart', traces, layout, {{ responsive: true }});
      }}

      function bindInputs() {{
        document.getElementById('dcSelect').addEventListener('change', render);
      }}

      updateSummary();
      bindInputs();
      render();
    </script>
  </body>
</html>
""".strip()

    return html_str
