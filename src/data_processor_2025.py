import json
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field
from tqdm.rich import tqdm


class Instance(BaseModel):
    id: str
    type: Literal["HN", "CN"]
    cpu: int
    gpu: int
    rdma: int
    memory: int
    disk_request: int
    disk_limit: int
    max_instance_per_node: int
    creation_time: int
    scheduled_time: int
    deletion_time: int

    def model_post_init(self, context: Any) -> None:
        assert self.creation_time <= self.scheduled_time <= self.deletion_time, self
        assert self.disk_request <= self.disk_limit, self


class App(BaseModel):
    name: str
    instances: list[Instance]

    @property
    def n_instances(self) -> int:
        return len(self.instances)

    def model_post_init(self, context: Any) -> None:
        """
        Assertions:
        - All the instance IDs are unique
        - All the HN jobs have the same number of gpu
        - Disk limits are the same for all the HN instances
        """
        instance_ids = [instance.id for instance in self.instances]
        if len(instance_ids) != len(set(instance_ids)):
            duplicates = {x for x in instance_ids if instance_ids.count(x) > 1}
            raise AssertionError(f"App {self.name}: duplicate instance IDs {sorted(duplicates)}")
        # all the HN jobs have the same number of gpu
        hn_gpus = {instance.gpu for instance in self.instances if instance.type == "HN"}
        if len(hn_gpus) > 1:
            raise AssertionError(f"App {self.name}: HN gpu mismatch {sorted(hn_gpus)}")
        # disk limits are the same for all the HN instances
        hn_disk_limits = {instance.disk_limit for instance in self.instances if instance.type == "HN"}
        if len(hn_disk_limits) > 1:
            raise AssertionError(f"App {self.name}: disk_limit mismatch HN {sorted(hn_disk_limits)}")


class Data2025(BaseModel):
    apps: list[App] = Field(default_factory=list)


def process_data_2025(input_data_path: Path) -> Data2025:
    df = pd.read_csv(input_data_path)
    max_time = max(df.creation_time.max(), df.scheduled_time.max(), df.deletion_time.max())

    apps: dict[str, list[Instance]] = dict()  # app_name -> App
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing csv"):
        assert row.cpu_request == row.cpu_limit, row
        assert row.gpu_request == row.gpu_limit, row
        assert row.rdma_request == row.rdma_limit, row
        assert row.memory_request == row.memory_limit, row
        instance = Instance(
            id=row.instance_sn,
            type=row.role,
            cpu=row.cpu_request,
            gpu=row.gpu_request,
            rdma=int(row.rdma_request),
            memory=int(row.memory_request),
            disk_request=int(row.disk_request),
            disk_limit=int(row.disk_limit),
            max_instance_per_node=int(row.max_instance_per_node),
            creation_time=int(row.creation_time if not pd.isna(row.creation_time) else 0),
            scheduled_time=int(row.scheduled_time if not pd.isna(row.scheduled_time) else 0),
            deletion_time=int(row.deletion_time if not pd.isna(row.deletion_time) else int(max_time)),
        )
        if row.app_name not in apps:
            apps[row.app_name] = [instance]
        else:
            apps[row.app_name].append(instance)

    ret: list[App] = []
    for app_name, instances in tqdm(apps.items(), desc="Processing apps"):
        ret.append(
            App(
                name=app_name,
                instances=instances,
            )
        )
    ret = sorted(ret, key=lambda x: x.name)

    logger.info(f"Processed {len(ret)} apps")

    return Data2025(apps=ret)


def visualize_data_2025(data: Data2025) -> str:
    """
    Visualize the data and return the HTML string

    Args:
        data: The data to visualize

    Returns:
        The HTML string
    """
    apps = data.apps
    total_apps = len(apps)
    total_instances = sum(app.n_instances for app in apps)
    # Assumptions for default load weights (Watts).
    # CPU: AMD Threadripper 3960X/3970X per-core power range 6–13.5W (use 10W).
    # GPU: NVIDIA A100 (assumed full power 400W per GPU).
    # RDMA/Memory/Disk: heuristic defaults (see assumptions list in HTML).
    default_w_cpu = 10.0
    default_w_gpu = 400.0
    # RDMA is stored as 0-100% per instance; 0.2W per 1% => 20W at 100%.
    default_w_rdma = 0.2
    # Memory and disk are per GiB; heuristics for magnitude.
    default_w_memory = 0.3  # ~10W per 32 GiB.
    default_w_disk = 0.006  # ~6W per 1 TiB.

    assumptions = [
        {
            "label": "CPU per-core power (assumed)",
            "value": f"{default_w_cpu} W per core",
            "note": "Assumed within the 6–13.5W per-core range for AMD Threadripper 3960X/3970X.",
            "url": "https://www.anandtech.com/show/15044/the-amd-ryzen-threadripper-3960x-and-3970x-review-24-and-32-cores-on-7nm/2",
        },
        {
            "label": "GPU power (assumed)",
            "value": f"{default_w_gpu} W per GPU",
            "note": "Assume NVIDIA A100 full power 400W.",
            "url": "https://massedcompute.com/faq-answers/?question=What%20are%20the%20estimated%20energy%20costs%20associated%20with%20powering%20NVIDIA%27s%20A100%20and%20H100%20GPUs%20in%20a%20data%20center?#:~:text=Power%20Consumption%20of%20A100%20and,1%2C000W%20under%20full%20load.",
        },
        {
            "label": "RDMA load (assumed)",
            "value": f"{default_w_rdma} W per 1% RDMA (20W at 100%)",
            "note": "Heuristic for a single RNIC; adjust if you have real NIC power specs.",
            "url": "",
        },
        {
            "label": "Memory load (assumed)",
            "value": f"{default_w_memory} W per GiB",
            "note": "Heuristic (~10W per 32 GiB).",
            "url": "",
        },
        {
            "label": "Disk load (assumed)",
            "value": f"{default_w_disk} W per GiB",
            "note": "Heuristic (~6W per 1 TiB).",
            "url": "",
        },
    ]
    assumptions_items: list[str] = []
    for item in assumptions:
        source = f' (<a href="{item["url"]}" target="_blank">source</a>)' if item["url"] else ""
        assumptions_items.append(f"<li><strong>{item['label']}</strong>: {item['value']} — {item['note']}{source}</li>")
    assumptions_html = "".join(assumptions_items)

    all_instances = [inst for app in apps for inst in app.instances]
    max_time = max((inst.deletion_time for inst in all_instances), default=0)

    def build_resource_series(instances: list[Instance]) -> dict[str, list[int]]:
        # Use scheduled_time when available (resource actually allocated), else creation_time.
        # If deletion_time is unknown (filled to max_time), we keep the instance "running"
        # until the trace end by NOT subtracting at max_time. This avoids the artificial drop.
        local_events: dict[int, dict[str, int]] = {}

        def add_event(t: int, cpu: int, gpu: int, rdma: int, memory: int, disk: int, n: int) -> None:
            if t not in local_events:
                local_events[t] = {
                    "cpu": 0,
                    "gpu": 0,
                    "rdma": 0,
                    "memory": 0,
                    "disk": 0,
                    "n": 0,
                }
            local_events[t]["cpu"] += cpu
            local_events[t]["gpu"] += gpu
            local_events[t]["rdma"] += rdma
            local_events[t]["memory"] += memory
            local_events[t]["disk"] += disk
            local_events[t]["n"] += n

        for inst in instances:
            start = inst.scheduled_time if inst.scheduled_time > 0 else inst.creation_time
            end = inst.deletion_time
            add_event(start, inst.cpu, inst.gpu, inst.rdma, inst.memory, inst.disk_limit, 1)
            if end != max_time:
                add_event(
                    end,
                    -inst.cpu,
                    -inst.gpu,
                    -inst.rdma,
                    -inst.memory,
                    -inst.disk_limit,
                    -1,
                )

        times = sorted(local_events.keys())
        running = {"cpu": 0, "gpu": 0, "rdma": 0, "memory": 0, "disk": 0, "n": 0}
        cpu_s: list[int] = []
        gpu_s: list[int] = []
        rdma_s: list[int] = []
        memory_s: list[int] = []
        disk_s: list[int] = []
        n_s: list[int] = []

        for t in times:
            delta = local_events[t]
            for k in running:
                running[k] += delta[k]
            cpu_s.append(running["cpu"])
            gpu_s.append(running["gpu"])
            rdma_s.append(running["rdma"])
            memory_s.append(running["memory"])
            disk_s.append(running["disk"])
            n_s.append(running["n"])

        return {
            "times": times,
            "cpu": cpu_s,
            "gpu": gpu_s,
            "rdma": rdma_s,
            "memory": memory_s,
            "disk": disk_s,
            "n": n_s,
        }

    total_series = build_resource_series(all_instances)
    app_series: dict[str, dict[str, list[int]]] = {app.name: build_resource_series(app.instances) for app in apps}

    min_time = min(total_series["times"], default=0)

    num_ends_at_trace_end = sum(1 for inst in all_instances if inst.deletion_time == max_time)
    data_payload = {
        "min_time": min_time,
        "max_time": max_time,
        "total_apps": total_apps,
        "total_instances": total_instances,
        "ends_at_trace_end": num_ends_at_trace_end,
        "total": total_series,
        "apps": app_series,
    }

    app_options = "".join(f'<option value="{name}">{name}</option>' for name in sorted(app_series.keys()))

    html_str = f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8"/>
    <title>DLRM Trace 2025 Load</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 24px; color: #1a1a1a; }}
      h1 {{ margin-bottom: 8px; }}
      .summary {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin: 16px 0; }}
      .card {{ padding: 12px 14px; border: 1px solid #e5e7eb; border-radius: 8px; }}
      .card h3 {{ margin: 0 0 6px 0; font-size: 14px; color: #555; }}
      .card p {{ margin: 0; font-size: 18px; font-weight: 600; }}
      .controls {{ display: grid; grid-template-columns: 2fr repeat(5, 1fr); gap: 12px; margin: 16px 0; align-items: end; }}
      .assumptions {{ margin: 16px 0; padding: 12px 14px; border: 1px solid #e5e7eb; border-radius: 8px; }}
      .assumptions h3 {{ margin: 0 0 8px 0; font-size: 14px; color: #555; }}
      .assumptions ul {{ margin: 0; padding-left: 18px; }}
      .assumptions li {{ margin: 6px 0; font-size: 13px; color: #333; }}
      .control {{ border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px; }}
      .control label {{ display: block; font-size: 12px; color: #555; margin-bottom: 6px; }}
      .control input, .control select {{ width: 100%; }}
      .note {{ margin-top: 8px; font-size: 12px; color: #666; }}
      #chart {{ width: 100%; height: 520px; }}
    </style>
  </head>
  <body>
    <h1>DLRM Trace 2025 Load Over Time</h1>
    <div class="summary">
      <div class="card"><h3>Apps</h3><p id="summaryApps">-</p></div>
      <div class="card"><h3>Instances</h3><p id="summaryInstances">-</p></div>
      <div class="card"><h3>Min Time</h3><p id="summaryMin">-</p></div>
      <div class="card"><h3>Max Time</h3><p id="summaryMax">-</p></div>
      <div class="card"><h3>Ends at Trace End</h3><p id="summaryEndsAtEnd">-</p></div>
    </div>

    <div class="controls">
      <div class="control">
        <label for="appSelect">App (overlay)</label>
        <select id="appSelect">
          {app_options}
        </select>
      </div>
      <div class="control">
        <label for="wCpu">W per CPU core</label>
        <input id="wCpu" type="number" value="{default_w_cpu}" step="0.1"/>
      </div>
      <div class="control">
        <label for="wGpu">W per GPU</label>
        <input id="wGpu" type="number" value="{default_w_gpu}" step="1"/>
      </div>
      <div class="control">
        <label for="wRdma">W per RDMA (1%)</label>
        <input id="wRdma" type="number" value="{default_w_rdma}" step="0.01"/>
      </div>
      <div class="control">
        <label for="wMemory">W per GiB memory</label>
        <input id="wMemory" type="number" value="{default_w_memory}" step="0.01"/>
      </div>
      <div class="control">
        <label for="wDisk">W per GiB disk</label>
        <input id="wDisk" type="number" value="{default_w_disk}" step="0.001"/>
      </div>
    </div>

    <div class="assumptions">
      <h3>Assumptions (defaults you can override above)</h3>
      <ul>
        {assumptions_html}
      </ul>
    </div>

    <div id="chart"></div>
    <div class="note">
      Load is computed as Σ(cpu*wCpu + gpu*wGpu + rdma*wRdma + mem*wMemory + disk*wDisk). Times are seconds since trace start.
      Start time uses <strong>scheduled_time</strong> when present; otherwise <strong>creation_time</strong>.
      Instances with <strong>deletion_time == trace end</strong> are treated as still running through the end (no subtraction at max_time) to avoid an artificial drop.
      Use Plotly interactions: drag to zoom, double-click to reset, hover for values.
    </div>

    <script>
      const DATA = {json.dumps(data_payload)};
      // Arbitrary anchor so we can use Plotly's date axis; times are offsets in seconds.
      const BASE_MS = Date.UTC(2025, 0, 1, 0, 0, 0);

      function getWeights() {{
        return {{
          cpu: parseFloat(document.getElementById('wCpu').value || '0'),
          gpu: parseFloat(document.getElementById('wGpu').value || '0'),
          rdma: parseFloat(document.getElementById('wRdma').value || '0'),
          memory: parseFloat(document.getElementById('wMemory').value || '0'),
          disk: parseFloat(document.getElementById('wDisk').value || '0'),
        }};
      }}

      function calcLoad(series, w) {{
        const n = series.times.length;
        const y = new Array(n);
        for (let i = 0; i < n; i++) {{
          y[i] = series.cpu[i] * w.cpu
               + series.gpu[i] * w.gpu
               + series.rdma[i] * w.rdma
               + series.memory[i] * w.memory
               + series.disk[i] * w.disk;
        }}
        return y;
      }}

      function updateSummary() {{
        document.getElementById('summaryApps').textContent = DATA.total_apps;
        document.getElementById('summaryInstances').textContent = DATA.total_instances;
        document.getElementById('summaryMin').textContent = DATA.min_time;
        document.getElementById('summaryMax').textContent = DATA.max_time;
        document.getElementById('summaryEndsAtEnd').textContent = DATA.ends_at_trace_end;
      }}

      function render() {{
        const w = getWeights();
        const appName = document.getElementById('appSelect').value;
        const total = DATA.total;
        const app = DATA.apps[appName];

        const totalX = total.times.map((t) => new Date(BASE_MS + t * 1000));
        const appX = app.times.map((t) => new Date(BASE_MS + t * 1000));

        const totalY = calcLoad(total, w);
        const appY = calcLoad(app, w);

        const gd = document.getElementById('chart');
        const existing = (gd && gd.data) ? gd.data : [];
        const totalVisible = existing[0] && existing[0].visible !== undefined ? existing[0].visible : true;
        const appVisible = existing[1] && existing[1].visible !== undefined ? existing[1].visible : true;

        const traces = [
          {{
            x: totalX,
            y: totalY,
            mode: 'lines',
            name: 'Total load (W)',
            line: {{ width: 2, color: '#1f77b4' }},
            hovertemplate: 't=%{{x}}<br>total=%{{y:.2f}} W<br>active=%{{customdata}}<extra></extra>',
            customdata: total.n,
            visible: totalVisible,
          }},
          {{
            x: appX,
            y: appY,
            mode: 'lines',
            name: 'App ' + appName + ' (W)',
            line: {{ width: 2, color: '#d62728' }},
            hovertemplate: 't=%{{x}}<br>app=%{{y:.2f}} W<br>active=%{{customdata}}<extra></extra>',
            customdata: app.n,
            visible: appVisible,
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
        const inputs = ['wCpu', 'wGpu', 'wRdma', 'wMemory', 'wDisk', 'appSelect'];
        inputs.forEach((id) => {{
          document.getElementById(id).addEventListener('input', render);
          document.getElementById(id).addEventListener('change', render);
        }});
      }}

      updateSummary();
      bindInputs();
      render();
    </script>
  </body>
</html>
""".strip()

    return html_str
