import argparse
from datetime import datetime, timedelta, timezone
from typing import Any

import gradio as gr
import plotly.graph_objects as go

from data_processor_2026 import Job, Spot2026, process_2026_spot


def _build_resource_series(jobs: list[Job], gpu_models: list[str]) -> dict[str, Any]:
    local_events: dict[int, dict[str, float]] = {}

    def add_event(t: int, cpu: float, gpu_model: str, gpu: float, n: int) -> None:
        if t not in local_events:
            local_events[t] = {"cpu": 0.0, "n": 0.0}
            for model in gpu_models:
                local_events[t][f"gpu:{model}"] = 0.0
        local_events[t]["cpu"] += cpu
        local_events[t]["n"] += n
        local_events[t][f"gpu:{gpu_model}"] += gpu

    for job in jobs:
        start = job.submit_time
        end = job.submit_time + job.duration
        cpu_total = float(job.cpu_num) * job.worker_num
        gpu_total = float(job.gpu_num) * job.worker_num
        add_event(start, cpu_total, job.gpu_model, gpu_total, 1)
        add_event(end, -cpu_total, job.gpu_model, -gpu_total, -1)

    times = sorted(local_events.keys())
    running = {"cpu": 0.0, "n": 0.0}
    for model in gpu_models:
        running[f"gpu:{model}"] = 0.0

    cpu_s: list[float] = []
    n_s: list[float] = []
    gpu_s: dict[str, list[float]] = {model: [] for model in gpu_models}

    for t in times:
        delta = local_events[t]
        for k in running:
            running[k] += delta[k]
        cpu_s.append(running["cpu"])
        n_s.append(running["n"])
        for model in gpu_models:
            gpu_s[model].append(running[f"gpu:{model}"])

    return {
        "times": times,
        "cpu": cpu_s,
        "n": n_s,
        "gpu": gpu_s,
    }


def _calc_load(series: dict[str, Any], gpu_models: list[str], w_cpu: float, w_gpu: dict[str, float]) -> list[float]:
    n = len(series["times"])
    y: list[float] = []
    for i in range(n):
        total = series["cpu"][i] * w_cpu
        for model in gpu_models:
            total += series["gpu"][model][i] * w_gpu.get(model, 0.0)
        y.append(total)
    return y


def _resolve_job_id(job_ids: list[int], jobs_by_name: dict[str, Job], raw: float | int) -> str:
    raw_int = int(raw)
    raw_name = str(raw_int)
    if raw_name in jobs_by_name:
        return raw_name
    if not job_ids:
        return next(iter(jobs_by_name.keys()))
    closest = min(job_ids, key=lambda v: abs(v - raw_int))
    return str(closest)


def _job_series(job: Job, min_time: int, max_time: int, w_cpu: float, w_gpu: float) -> tuple[list[datetime], list[float], list[int]]:
    start = job.submit_time
    end = job.submit_time + job.duration
    total_cpu = job.cpu_num * job.worker_num
    total_gpu = job.gpu_num * job.worker_num
    power = total_cpu * w_cpu + total_gpu * w_gpu
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    x = [
        base + timedelta(seconds=min_time),
        base + timedelta(seconds=start),
        base + timedelta(seconds=end),
        base + timedelta(seconds=max_time),
    ]
    y = [0.0, power, power, 0.0]
    n = [0, job.worker_num, job.worker_num, 0]
    return x, y, n


def build_app(data: Spot2026) -> gr.Blocks:
    jobs = data.jobs
    nodes = data.nodes

    total_jobs = len(jobs)
    total_nodes = len(nodes)
    total_gpu_capacity = sum(node.gpu_num for node in nodes)
    total_cpu_capacity = sum(node.cpu_num for node in nodes)

    min_time = min((job.submit_time for job in jobs), default=0)
    max_time = max((job.submit_time + job.duration for job in jobs), default=0)

    gpu_models = sorted({job.gpu_model for job in jobs} | {node.gpu_model for node in nodes})

    default_w_cpu = 4.0
    default_w_gpu_by_model = {
        "A800-SXM4-80GB": 350.0,
        "A100-SXM4-80GB": 400.0,
        "A10": 150.0,
        "GPU-series-1": 400.0,
        "GPU-series-2": 700.0,
        "H800": 400.0,
    }
    for model in gpu_models:
        default_w_gpu_by_model.setdefault(model, 300.0)

    total_series = _build_resource_series(jobs, gpu_models)

    jobs_by_name = {job.name: job for job in jobs}
    job_ids_numeric = sorted(int(name) for name in jobs_by_name.keys() if str(name).isdigit())
    min_job_id = min(job_ids_numeric) if job_ids_numeric else 0
    max_job_id = max(job_ids_numeric) if job_ids_numeric else 0

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def render(
        job_id: float,
        w_cpu: float,
        visible_traces: list[str],
        *gpu_weights: float,
    ) -> tuple[go.Figure, str, str]:
        w_gpu = {model: weight for model, weight in zip(gpu_models, gpu_weights)}
        total_y = _calc_load(total_series, gpu_models, w_cpu, w_gpu)
        total_x = [base + timedelta(seconds=t) for t in total_series["times"]]

        resolved_job = _resolve_job_id(job_ids_numeric, jobs_by_name, job_id)
        job = jobs_by_name[resolved_job]
        job_x, job_y, job_n = _job_series(job, min_time, max_time, w_cpu, w_gpu.get(job.gpu_model, 0.0))

        fig = go.Figure()
        show_total = "Total load" in visible_traces
        show_job = "Selected job" in visible_traces

        fig.add_trace(
            go.Scatter(
                x=total_x,
                y=total_y,
                mode="lines",
                name="Total load (W)",
                line=dict(width=2, color="#1f77b4"),
                hovertemplate="t=%{x}<br>total=%{y:.2f} W<br>active jobs=%{customdata}<extra></extra>",
                customdata=total_series["n"],
                visible=True if show_total else "legendonly",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=job_x,
                y=job_y,
                mode="lines",
                name=f"Job {resolved_job} (W)",
                line=dict(width=2, color="#d62728"),
                hovertemplate="t=%{x}<br>job=%{y:.2f} W<br>workers=%{customdata}<extra></extra>",
                customdata=job_n,
                visible=True if show_job else "legendonly",
            )
        )
        fig.update_layout(
            margin=dict(l=60, r=20, t=30, b=50),
            xaxis=dict(title="time", type="date", rangeslider=dict(visible=True)),
            yaxis=dict(title="estimated power (W)"),
            legend=dict(orientation="h"),
        )

        job_stats = (
            f"**Job stats**  \n"
            f"- Total jobs: {total_jobs}  \n"
            f"- Min time: {min_time}  \n"
            f"- Max time: {max_time}  \n"
            f"**Selected job**  \n"
            f"- Job ID: {resolved_job}  \n"
            f"- Type: {job.type}  \n"
            f"- GPU model: {job.gpu_model}  \n"
            f"- Workers: {job.worker_num}  \n"
            f"- GPU per worker: {job.gpu_num}  \n"
            f"- CPU per worker: {job.cpu_num}  \n"
            f"- Duration (s): {job.duration}"
        )

        node_stats = f"**Node stats**  \n- Total nodes: {total_nodes}  \n- Total GPUs: {total_gpu_capacity}  \n- Total CPUs: {total_cpu_capacity}"

        return fig, job_stats, node_stats

    with gr.Blocks(title="Spot GPU Trace 2026") as demo:
        gr.Markdown("# Spot GPU Trace 2026 Load Over Time")
        with gr.Row():
            job_input = gr.Number(label="Job ID (overlay)", value=min_job_id, precision=0)
            w_cpu = gr.Number(label="W per CPU core", value=default_w_cpu)
            visible_traces = gr.CheckboxGroup(
                choices=["Total load", "Selected job"],
                value=["Total load", "Selected job"],
                label="Visible traces",
            )
        with gr.Row():
            gpu_inputs = [gr.Number(label=f"W per GPU ({model})", value=default_w_gpu_by_model[model]) for model in gpu_models]
        with gr.Row():
            plot = gr.Plot()
        with gr.Row():
            job_stats_md = gr.Markdown()
            node_stats_md = gr.Markdown()
        gr.Markdown(
            "Load is computed as sum(cpu*wCpu + sum(gpu_model*wGpu_model)). "
            "Job load assumes each worker requests cpu_request and gpu_request per worker for the job duration."
        )

        inputs = [job_input, w_cpu, visible_traces] + gpu_inputs
        job_input.change(render, inputs=inputs, outputs=[plot, job_stats_md, node_stats_md])
        w_cpu.change(render, inputs=inputs, outputs=[plot, job_stats_md, node_stats_md])
        visible_traces.change(render, inputs=inputs, outputs=[plot, job_stats_md, node_stats_md])
        for comp in gpu_inputs:
            comp.change(render, inputs=inputs, outputs=[plot, job_stats_md, node_stats_md])
        demo.load(render, inputs=inputs, outputs=[plot, job_stats_md, node_stats_md])

    return demo


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gradio app for 2026 spot GPU trace visualization.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--data-json",
        default="outputs/2026_spot/data.json",
        help="Path to exported data.json (server-side load).",
    )
    parser.add_argument(
        "--job-info",
        default="data/clusterdata/cluster-trace-v2026-spot-gpu/job_info_df.csv",
        help="Path to job_info_df.csv",
    )
    parser.add_argument(
        "--node-info",
        default="data/clusterdata/cluster-trace-v2026-spot-gpu/node_info_df.csv",
        help="Path to node_info_df.csv",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind.")
    parser.add_argument("--port", type=int, default=12345, help="Port to bind.")
    parser.add_argument(
        "--share",
        action="store_true",
        help="Enable Gradio share link.",
    )
    args = parser.parse_args()

    data: Spot2026
    try:
        with open(args.data_json, "r") as f:
            data = Spot2026.model_validate_json(f.read())
    except FileNotFoundError:
        data = process_2026_spot(
            job_info_path=args.job_info,
            node_info_path=args.node_info,
        )
    demo = build_app(data)
    demo.launch(server_name=args.host, server_port=args.port, share=args.share)


if __name__ == "__main__":
    main()
