from pathlib import Path

import typer
from loguru import logger
from pydantic import BaseModel

from src.data_processor_2025 import process_data_2025, visualize_data_2025
from src.data_processor_2026 import process_2026_spot, visualize_2026_spot
from src.data_processor_ukdata import process_ukdata, visualize_ukdata
from src.pf_simulation_analysis import run_pf_simulation_and_analysis
from src.research_spot_trace_volatility import analyze_volatility
from src.research_transmission_impact import run_transmission_pf_timeseries_simulation


class GloablConfig(BaseModel):
    clusterdata_root: Path = Path("./data/clusterdata")
    output_root: Path = Path("./outputs")

    @property
    def clusterdata_2025(self) -> Path:
        return self.clusterdata_root / "cluster-trace-gpu-v2025" / "disaggregated_DLRM_trace.csv"

    @property
    def output_2025(self) -> Path:
        return self.output_root / "2025"

    @property
    def clusterdata_2026_spot_job(self) -> Path:
        return self.clusterdata_root / "cluster-trace-v2026-spot-gpu" / "job_info_df.csv"

    @property
    def clusterdata_2026_spot_node(self) -> Path:
        return self.clusterdata_root / "cluster-trace-v2026-spot-gpu" / "node_info_df.csv"

    @property
    def output_2026_spot(self) -> Path:
        return self.output_root / "2026_spot"

    @property
    def clusterdata_ukdata(self) -> Path:
        return Path("./data/ukpn-data-centre-demand-profiles.csv")

    @property
    def output_ukdata(self) -> Path:
        return self.output_root / "ukdata"

    @property
    def output_pf_simulation_and_analysis(self) -> Path:
        return self.output_root / "power_flow"

    @property
    def output_volatility(self) -> Path:
        return self.output_root / "volatility"

    @property
    def output_transmission(self) -> Path:
        return self.output_root / "transmission"


app = typer.Typer()
config = GloablConfig()


@app.command("preprocess_2025")
def preprocess_2020_trace(
    trace_path: Path = typer.Option(config.clusterdata_2025, help="Path to the trace file"),
    output_path: Path = typer.Option(config.output_2025, help="Path to the output file"),
) -> None:
    """
    Preprocess the 2025 trace from Alibaba's Clusterdata and save the data to a JSON file and generate a HTML file.
    """
    data = process_data_2025(trace_path)
    html_str = visualize_data_2025(data)
    output_path.mkdir(parents=True, exist_ok=True)
    with open(output_path / "data.json", "w") as f:
        f.write(data.model_dump_json(indent=2))
    with open(output_path / "data_visualization.html", "w") as f:
        f.write(html_str)
    logger.info(f"Preprocessed 2025 trace and saved to {output_path}")

    return None


@app.command("preprocess_2026_spot")
def preprocess_2026_spot(
    job_info_path: Path = typer.Option(config.clusterdata_2026_spot_job, help="Path to the job info file"),
    node_info_path: Path = typer.Option(config.clusterdata_2026_spot_node, help="Path to the node info file"),
    output_path: Path = typer.Option(config.output_2026_spot, help="Path to the output file"),
) -> None:
    """
    Preprocess the 2026 spot trace from Alibaba's Clusterdata and save the data to a JSON file and generate a HTML file.
    """
    data = process_2026_spot(job_info_path, node_info_path)
    html_str = visualize_2026_spot(data)
    output_path.mkdir(parents=True, exist_ok=True)
    with open(output_path / "data.json", "w") as f:
        f.write(data.model_dump_json(indent=2))
    with open(output_path / "data_visualization.html", "w") as f:
        f.write(html_str)
    logger.info(f"Preprocessed 2026 spot trace and saved to {output_path}")

    return None


@app.command("preprocess_ukdata")
def preprocess_ukdata_cmd(
    data_path: Path = typer.Option(config.clusterdata_ukdata, help="Path to the UK data file"),
    output_path: Path = typer.Option(config.output_ukdata, help="Path to the output directory"),
) -> None:
    """
    Preprocess the UK data center demand profiles and save the data to a JSON file and generate a HTML file.
    """
    data = process_ukdata(data_path)
    html_str = visualize_ukdata(data)
    output_path.mkdir(parents=True, exist_ok=True)
    with open(output_path / "data.json", "w") as f:
        f.write(data.model_dump_json(indent=2))
    with open(output_path / "data_visualization.html", "w") as f:
        f.write(html_str)
    logger.info(f"Preprocessed UK data and saved to {output_path}")

    return None


@app.command("pf_simulation_and_analysis")
def pf_simulation_and_analysis(
    input_path: Path = typer.Option(config.output_ukdata / "data.json", help="Path to the UK data JSON file"),
    output_path: Path = typer.Option(config.output_pf_simulation_and_analysis, help="Path to the output directory"),
    network_name: str = typer.Option("case33bw", help="Name of the network to use"),
    node_idx: int = typer.Option(18, help="Index of the node to connect the data center"),
    peak_mw: float = typer.Option(5.0, help="Assumed peak power of the data center in MW"),
    hours: int = typer.Option(365 * 24, help="Number of hours to simulate"),
) -> None:
    """
    Run a power flow simulation and analysis using UK data center profiles.
    """
    run_pf_simulation_and_analysis(
        data_json_path=input_path,
        output_dir=output_path,
        network_name=network_name,
        node_idx=node_idx,
        peak_mw=peak_mw,
        hours=hours,
    )
    return None


@app.command("analyze_2026_volatility")
def analyze_2026_volatility(
    input_path: Path = typer.Option(config.output_2026_spot / "data.json", help="Path to the 2026 spot trace JSON"),
    output_path: Path = typer.Option(config.output_volatility, help="Path to the output directory"),
) -> None:
    """
    Analyze the volatility of the 2026 spot trace aggregate power curve.
    """
    analyze_volatility(
        data_path=input_path,
        output_dir=output_path,
    )
    return None


@app.command("pf_transmission_timeseries")
def run_transmission_pf_timeseries(
    input_path: Path = typer.Option(config.output_volatility / "aggregated_load_profile.csv", help="Path to the aggregated volatility profile CSV"),
    output_path: Path = typer.Option(config.output_transmission, help="Path to the output directory"),
    network_name: str = typer.Option("case39", help="Name of the network to use"),
    grid_bus_idx: int = typer.Option(16, help="Bus index on the IEEE 39-bus system to connect the load"),
    peak_load_mw: float = typer.Option(1000.0, help="Assumed peak power of the AI data center in MW"),
) -> None:
    """
    Analyze the transmission grid impact of an AI data center load profile.
    """
    run_transmission_pf_timeseries_simulation(
        load_profile_path=input_path,
        output_dir=output_path,
        network_name=network_name,
        grid_bus_idx=grid_bus_idx,
        peak_load_mw=peak_load_mw,
    )
    return None


if __name__ == "__main__":
    app()
