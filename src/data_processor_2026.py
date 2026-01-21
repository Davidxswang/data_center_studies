import json
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field
from tqdm.rich import tqdm


class Node(BaseModel):
    name: str
    gpu_model: str
    gpu_num: int = Field(gt=0)
    cpu_num: int = Field(gt=0)


class Job(BaseModel):
    name: str
    type: Literal["HP", "Spot"]
    organization: str
    gpu_model: str
    cpu_num: float = Field(ge=0)
    gpu_num: float = Field(gt=0)
    worker_num: int = Field(gt=0)
    submit_time: int = Field(ge=0)
    duration: int = Field(gt=0)


class Spot2026(BaseModel):
    jobs: list[Job]
    nodes: list[Node]

    def model_post_init(self, context: Any) -> None:
        # node name should be unique
        node_names = [node.name for node in self.nodes]
        if len(node_names) != len(set(node_names)):
            raise AssertionError("Node names are not unique")
        # print the unique gpu models
        unique_gpu_models = list(set([node.gpu_model for node in self.nodes]))
        logger.info(f"Unique GPU models in nodes: {unique_gpu_models}")
        # print unique gpu nums in nodes
        unique_gpu_nums = list(set([node.gpu_num for node in self.nodes]))
        logger.info(f"Unique GPU nums in nodes: {unique_gpu_nums}")
        # print unique cpu nums in nodes
        unique_cpu_nums = list(set([node.cpu_num for node in self.nodes]))
        logger.info(f"Unique CPU nums in nodes: {unique_cpu_nums}")
        # job name should be unique
        job_names = [job.name for job in self.jobs]
        if len(job_names) != len(set(job_names)):
            raise AssertionError("Job names are not unique")
        # gpu models from jobs should be in the gpu models in nodes
        unique_gpu_models_from_jobs = list(set([job.gpu_model for job in self.jobs]))
        if not set(unique_gpu_models_from_jobs).issubset(set(unique_gpu_models)):
            raise AssertionError("GPU models from jobs are not in the GPU models in nodes")
        # cpu and gpu nums from jobs should be <= their max in nodes
        max_cpu, max_gpu = max([node.cpu_num for node in self.nodes]), max([node.gpu_num for node in self.nodes])
        for job in self.jobs:
            if job.cpu_num > max_cpu or job.gpu_num > max_gpu:
                raise AssertionError(f"CPU or GPU nums from job {job.name} are greater than the max in nodes")


def process_2026_spot(job_info_path: Path, node_info_path: Path) -> Spot2026:
    df_node = pd.read_csv(node_info_path)
    df_job = pd.read_csv(job_info_path)

    nodes: list[Node] = []
    for _, row in tqdm(df_node.iterrows(), total=len(df_node), desc="Processing nodes"):
        nodes.append(
            Node(
                name=str(row["node_name"]),
                gpu_model=row["gpu_model"],
                gpu_num=row["gpu_capacity_num"],
                cpu_num=row["cpu_num"],
            )
        )

    jobs: list[Job] = []
    for _, row in tqdm(df_job.iterrows(), total=len(df_job), desc="Processing jobs"):
        jobs.append(
            Job(
                name=str(row["job_name"]),
                type=row["job_type"],
                organization=str(row["organization"]),
                gpu_model=row["gpu_model"],
                cpu_num=row["cpu_request"],
                gpu_num=row["gpu_request"],
                worker_num=row["worker_num"],
                submit_time=int(row["submit_time"]),
                duration=int(row["duration"]),
            )
        )

    ret = Spot2026(jobs=jobs, nodes=nodes)
    logger.info(f"Processed {len(jobs)} jobs and {len(nodes)} nodes")

    return ret


def visualize_2026_spot(data: Spot2026) -> str:
    """
    Visualize the data and return the HTML string.

    Args:
        data: The data to visualize

    Returns:
        The HTML string
    """
    jobs = data.jobs
    nodes = data.nodes

    total_jobs = len(jobs)
    total_nodes = len(nodes)
    total_gpu_capacity = sum(node.gpu_num for node in nodes)
    total_cpu_capacity = sum(node.cpu_num for node in nodes)

    min_time = min((job.submit_time for job in jobs), default=0)
    max_time = max((job.submit_time + job.duration for job in jobs), default=0)

    gpu_models = sorted({job.gpu_model for job in jobs} | {node.gpu_model for node in nodes})

    # Assumptions for default load weights (Watts).
    # CPU per-core power estimated from EPYC 7763/9654 TDP per core.
    default_w_cpu = 4.0
    default_w_gpu_by_model = {
        "A800-SXM4-80GB": 350.0,  # use A800 PCIe 80GB board power as proxy.
        "A100-SXM4-80GB": 400.0,
        "A10": 150.0,
        "GPU-series-1": 400.0,  # anonymized; assume A100-class SXM4 TDP.
        "GPU-series-2": 700.0,  # anonymized; assume H100-class SXM TDP.
        "H800": 400.0,  # use H800 PCIe Gen5 max power as proxy.
    }
    default_w_gpu_by_model.update({model: default_w_gpu_by_model.get(model, 300.0) for model in gpu_models})

    assumptions = [
        {
            "label": "CPU per-core power (assumed)",
            "value": f"{default_w_cpu} W per core",
            "note": "Estimated from EPYC 7763 (280W/64c ~= 4.38W) and EPYC 9654 (360W/96c ~= 3.75W).",
            "url": "",
        },
        {
            "label": "EPYC 7763 TDP reference",
            "value": "280W TDP (64 cores)",
            "note": "Used to estimate per-core power for 128 vCPU nodes.",
            "url": "https://www.amd.com/en/products/cpu/amd-epyc-7763",
        },
        {
            "label": "EPYC 9654 TDP reference",
            "value": "360W TDP (96 cores)",
            "note": "Used to estimate per-core power for 192 vCPU nodes.",
            "url": "https://www.amd.com/en/products/cpu/amd-epyc-9654",
        },
        {
            "label": "A100-SXM4-80GB power",
            "value": f"{default_w_gpu_by_model['A100-SXM4-80GB']} W per GPU",
            "note": "Max TDP for A100 SXM4 is 400W.",
            "url": "https://www.nvidia.com/en-us/data-center/a100/",
        },
        {
            "label": "A800-SXM4-80GB power",
            "value": f"{default_w_gpu_by_model['A800-SXM4-80GB']} W per GPU",
            "note": "Using A800 PCIe 80GB board power as proxy for A800 SXM4.",
            "url": "https://www.techpowerup.com/gpu-specs/a800-pcie-80-gb.c3965",
        },
        {
            "label": "A10 power",
            "value": f"{default_w_gpu_by_model['A10']} W per GPU",
            "note": "Max TDP 150W on NVIDIA A10 product page.",
            "url": "https://www.nvidia.com/en-us/data-center/products/a10-gpu/",
        },
        {
            "label": "H800 power",
            "value": f"{default_w_gpu_by_model['H800']} W per GPU",
            "note": "Max power consumption for H800 PCIe Gen5.",
            "url": "https://lenovopress.lenovo.com/lp1814-thinksystem-nvidia-h800-pcie-gen5-gpu",
        },
        {
            "label": "GPU-series-1 power (assumed)",
            "value": f"{default_w_gpu_by_model['GPU-series-1']} W per GPU",
            "note": "Model anonymized in dataset; assume A100-class SXM4 TDP.",
            "url": "https://www.nvidia.com/en-us/data-center/a100/",
        },
        {
            "label": "GPU-series-2 power (assumed)",
            "value": f"{default_w_gpu_by_model['GPU-series-2']} W per GPU",
            "note": "Model anonymized in dataset; assume H100-class SXM TDP.",
            "url": "https://www.nvidia.com/en-us/data-center/h100/",
        },
        {
            "label": "CPU core counts in nodes (assumed)",
            "value": "192/128/126 vCPUs",
            "note": "Likely 96-core, 64-core, and 63-core CPUs with SMT; per-core power used above.",
            "url": "",
        },
    ]
    assumptions_items: list[str] = []
    for item in assumptions:
        source = f' (<a href="{item["url"]}" target="_blank">source</a>)' if item["url"] else ""
        assumptions_items.append(f"<li><strong>{item['label']}</strong>: {item['value']} - {item['note']}{source}</li>")
    assumptions_html = "".join(assumptions_items)

    def build_resource_series(jobs_list: list[Job]) -> dict[str, Any]:
        local_events: dict[int, dict[str, float]] = {}

        def add_event(t: int, cpu: float, gpu_model: str, gpu: float, n: int) -> None:
            if t not in local_events:
                local_events[t] = {"cpu": 0.0, "n": 0.0}
                for model in gpu_models:
                    local_events[t][f"gpu:{model}"] = 0.0
            local_events[t]["cpu"] += cpu
            local_events[t]["n"] += n
            local_events[t][f"gpu:{gpu_model}"] += gpu

        for job in jobs_list:
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

    total_series = build_resource_series(jobs)

    job_meta = {
        job.name: {
            "name": job.name,
            "type": job.type,
            "organization": job.organization,
            "gpu_model": job.gpu_model,
            "cpu_num": job.cpu_num,
            "gpu_num": job.gpu_num,
            "worker_num": job.worker_num,
            "submit_time": job.submit_time,
            "duration": job.duration,
        }
        for job in jobs
    }

    job_ids_numeric = sorted(int(name) for name in job_meta.keys() if str(name).isdigit())
    min_job_id = min(job_ids_numeric) if job_ids_numeric else 0
    max_job_id = max(job_ids_numeric) if job_ids_numeric else 0

    data_payload = {
        "min_time": min_time,
        "max_time": max_time,
        "total_jobs": total_jobs,
        "total_nodes": total_nodes,
        "total_gpu_capacity": total_gpu_capacity,
        "total_cpu_capacity": total_cpu_capacity,
        "gpu_models": gpu_models,
        "total": total_series,
        "jobs": job_meta,
        "job_ids": job_ids_numeric,
        "min_job_id": min_job_id,
        "max_job_id": max_job_id,
    }

    gpu_inputs = []
    for model in gpu_models:
        value = default_w_gpu_by_model.get(model, 300.0)
        safe_id = model.replace(" ", "_").replace("/", "_")
        gpu_inputs.append(
            f"""
      <div class="control">
        <label for="wGpu_{safe_id}">W per GPU ({model})</label>
        <input id="wGpu_{safe_id}" type="number" value="{value}" step="1"/>
      </div>
            """.strip()
        )
    gpu_inputs_html = "\n".join(gpu_inputs)

    html_str = f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8"/>
    <title>Spot GPU Trace 2026 Load</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 24px; color: #1a1a1a; }}
      h1 {{ margin-bottom: 8px; }}
      .summary {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin: 16px 0; }}
      .summary h2 {{ grid-column: 1 / -1; margin: 0 0 6px 0; font-size: 14px; color: #555; }}
      .card {{ padding: 12px 14px; border: 1px solid #e5e7eb; border-radius: 8px; }}
      .card h3 {{ margin: 0 0 6px 0; font-size: 14px; color: #555; }}
      .card p {{ margin: 0; font-size: 18px; font-weight: 600; }}
      .controls {{ display: grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap: 12px; margin: 16px 0; align-items: end; }}
      .assumptions {{ margin: 16px 0; padding: 12px 14px; border: 1px solid #e5e7eb; border-radius: 8px; }}
      .assumptions h3 {{ margin: 0 0 8px 0; font-size: 14px; color: #555; }}
      .assumptions ul {{ margin: 0; padding-left: 18px; }}
      .assumptions li {{ margin: 6px 0; font-size: 13px; color: #333; }}
      .control {{ border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px; }}
      .control label {{ display: block; font-size: 12px; color: #555; margin-bottom: 6px; }}
      .control input, .control select {{ width: 100%; }}
      .sources {{ margin: 16px 0; padding: 12px 14px; border: 1px solid #e5e7eb; border-radius: 8px; }}
      .sources h3 {{ margin: 0 0 8px 0; font-size: 14px; color: #555; }}
      .sources ul {{ margin: 0; padding-left: 18px; }}
      .sources li {{ margin: 6px 0; font-size: 13px; color: #333; }}
      #chart {{ width: 100%; height: 520px; }}
      .legend {{ margin-top: 8px; font-size: 12px; color: #666; }}
      .note {{ margin-top: 8px; font-size: 12px; color: #666; }}
      @media (max-width: 980px) {{
        .summary {{ grid-template-columns: repeat(3, 1fr); }}
        .controls {{ grid-template-columns: repeat(2, minmax(160px, 1fr)); }}
      }}
    </style>
  </head>
  <body>
    <h1>Spot GPU Trace 2026 Load Over Time</h1>
    <div class="summary">
      <h2>Job stats</h2>
      <div class="card"><h3>Total Jobs</h3><p id="summaryJobs">-</p></div>
      <div class="card"><h3>Min Time</h3><p id="summaryMin">-</p></div>
      <div class="card"><h3>Max Time</h3><p id="summaryMax">-</p></div>
    </div>
    <div class="summary">
      <h2>Node stats</h2>
      <div class="card"><h3>Total Nodes</h3><p id="summaryNodes">-</p></div>
      <div class="card"><h3>Total GPUs</h3><p id="summaryGpus">-</p></div>
      <div class="card"><h3>Total CPUs</h3><p id="summaryCpus">-</p></div>
    </div>

    <div class="controls">
      <div class="control">
        <label for="jobInput">Job ID (overlay)</label>
        <input id="jobInput" type="number" min="{min_job_id}" max="{max_job_id}" step="1" value="{min_job_id}"/>
      </div>
      <div class="control">
        <label for="wCpu">W per CPU core</label>
        <input id="wCpu" type="number" value="{default_w_cpu}" step="0.1"/>
      </div>
      {gpu_inputs_html}
    </div>

    <div class="assumptions">
      <h3>Assumptions (defaults you can override above)</h3>
      <ul>
        {assumptions_html}
      </ul>
    </div>

    <div class="sources">
      <h3>Data sources</h3>
      <ul>
        <li><strong>Node inventory:</strong> clusterdata/cluster-trace-v2026-spot-gpu/node_info_df.csv</li>
        <li><strong>Job workload:</strong> clusterdata/cluster-trace-v2026-spot-gpu/job_info_df.csv</li>
        <li><strong>Dataset README:</strong> clusterdata/cluster-trace-v2026-spot-gpu/README.md</li>
      </ul>
    </div>

    <div id="chart"></div>
    <div class="note">
      Load is computed as sum(cpu*wCpu + sum(gpu_model*wGpu_model)). Times are seconds since trace start.
      Job load assumes each worker requests cpu_request and gpu_request per worker for the job duration.
      Use Plotly interactions: drag to zoom, double-click to reset, hover for values.
    </div>

    <script>
      const DATA = {json.dumps(data_payload)};
      const BASE_MS = Date.UTC(2026, 0, 1, 0, 0, 0);

      function getWeights() {{
        const weights = {{
          cpu: parseFloat(document.getElementById('wCpu').value || '0'),
          gpu: {{}},
        }};
        DATA.gpu_models.forEach((model) => {{
          const safeId = model.replace(/\\s+/g, '_').replace(/\\//g, '_');
          const input = document.getElementById('wGpu_' + safeId);
          weights.gpu[model] = parseFloat((input && input.value) || '0');
        }});
        return weights;
      }}

      function calcLoad(series, w) {{
        const n = series.times.length;
        const y = new Array(n);
        for (let i = 0; i < n; i++) {{
          let total = series.cpu[i] * w.cpu;
          DATA.gpu_models.forEach((model) => {{
            total += series.gpu[model][i] * w.gpu[model];
          }});
          y[i] = total;
        }}
        return y;
      }}

      function getSelectedJobName() {{
        const input = document.getElementById('jobInput');
        const raw = (input && input.value || '').trim();
        if (DATA.jobs[raw]) {{
          return raw;
        }}
        const numeric = parseInt(raw, 10);
        if (!Number.isFinite(numeric) || DATA.job_ids.length === 0) {{
          return Object.keys(DATA.jobs)[0];
        }}
        let closest = DATA.job_ids[0];
        let minDelta = Math.abs(closest - numeric);
        for (let i = 1; i < DATA.job_ids.length; i++) {{
          const id = DATA.job_ids[i];
          const delta = Math.abs(id - numeric);
          if (delta < minDelta) {{
            minDelta = delta;
            closest = id;
          }}
        }}
        const resolved = String(closest);
        if (input) {{
          input.value = resolved;
        }}
        return resolved;
      }}

      function buildJobSeries(jobName) {{
        const job = DATA.jobs[jobName];
        if (!job) {{
          return {{ x: [], y: [], n: [] }};
        }}
        const w = getWeights();
        const start = job.submit_time;
        const end = job.submit_time + job.duration;
        const totalCpu = job.cpu_num * job.worker_num;
        const totalGpu = job.gpu_num * job.worker_num;
        const gpuPower = (w.gpu[job.gpu_model] || 0) * totalGpu;
        const power = totalCpu * w.cpu + gpuPower;
        const x = [new Date(BASE_MS + DATA.min_time * 1000), new Date(BASE_MS + start * 1000), new Date(BASE_MS + end * 1000), new Date(BASE_MS + DATA.max_time * 1000)];
        const y = [0, power, power, 0];
        const n = [0, job.worker_num, job.worker_num, 0];
        return {{ x, y, n }};
      }}

      function updateSummary() {{
        document.getElementById('summaryJobs').textContent = DATA.total_jobs;
        document.getElementById('summaryNodes').textContent = DATA.total_nodes;
        document.getElementById('summaryGpus').textContent = DATA.total_gpu_capacity;
        document.getElementById('summaryCpus').textContent = DATA.total_cpu_capacity;
        document.getElementById('summaryMin').textContent = DATA.min_time;
        document.getElementById('summaryMax').textContent = DATA.max_time;
      }}

      function renderChart() {{
        const w = getWeights();
        const total = DATA.total;
        const totalX = total.times.map((t) => new Date(BASE_MS + t * 1000));
        const totalY = calcLoad(total, w);

        const jobName = getSelectedJobName();
        const jobSeries = buildJobSeries(jobName);

        const gd = document.getElementById('chart');
        const existing = (gd && gd.data) ? gd.data : [];
        const totalVisible = existing[0] && existing[0].visible !== undefined ? existing[0].visible : true;
        const jobVisible = existing[1] && existing[1].visible !== undefined ? existing[1].visible : true;

        const traces = [
          {{
            x: totalX,
            y: totalY,
            mode: 'lines',
            name: 'Total load (W)',
            line: {{ width: 2, color: '#1f77b4' }},
            hovertemplate: 't=%{{x}}<br>total=%{{y:.2f}} W<br>active jobs=%{{customdata}}<extra></extra>',
            customdata: total.n,
            visible: totalVisible,
          }},
          {{
            x: jobSeries.x,
            y: jobSeries.y,
            mode: 'lines',
            name: 'Job ' + jobName + ' (W)',
            line: {{ width: 2, color: '#d62728' }},
            hovertemplate: 't=%{{x}}<br>job=%{{y:.2f}} W<br>workers=%{{customdata}}<extra></extra>',
            customdata: jobSeries.n,
            visible: jobVisible,
          }},
        ];

        const layout = {{
          margin: {{ l: 60, r: 20, t: 30, b: 50 }},
          xaxis: {{ title: 'time', type: 'date', rangeslider: {{ visible: true }} }},
          yaxis: {{ title: 'estimated power (W)' }},
          legend: {{ orientation: 'h' }},
        }};

        Plotly.react('chart', traces, layout, {{ responsive: true }});
      }}

      function bindInputs() {{
        const inputs = ['wCpu', 'jobInput'];
        DATA.gpu_models.forEach((model) => {{
          const safeId = model.replace(/\\s+/g, '_').replace(/\\//g, '_');
          inputs.push('wGpu_' + safeId);
        }});
        inputs.forEach((id) => {{
          const el = document.getElementById(id);
          if (!el) return;
          el.addEventListener('input', () => {{ renderChart(); }});
          el.addEventListener('change', () => {{ renderChart(); }});
        }});
      }}

      updateSummary();
      bindInputs();
      renderChart();
    </script>
  </body>
</html>
""".strip()

    return html_str
