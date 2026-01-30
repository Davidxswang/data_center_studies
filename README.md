# data_center_studies

Studies of data center load patterns, generate new research ideas, try new methods. Internal use only.

## Setup

### Get the Dataset Into Place

1. Clone the Alibaba's Clusterdata:

```bash
mkdir data
cd data
git clone git@github.com:alibaba/clusterdata.git
```

2. Put the UK Data Center Dataset into `data`, so it should be in: `data/`

### Install Dependencies

1. Install UV

If you haven't installed UV, install UV:

```bash
wget -qO- https://astral.sh/uv/install.sh | sh
```

2. Sync

```bash
uv sync
```

3. How to use

Every time you want to run some python command you can run like option 1 or option 2:

Option 1:

```bash
uv run python main.py xxx
```

Option 2:

```bash
source .venv/bin/activate
python main.py xxx
```

---

You can check the help:

```bash
uv run python main.py --help
```

OR

```bash
source .venv/bin/activate
python main.py --help
```

### Create Visualization of the Dataset

```bash
uv run main.py preprocess_2025
uv run main.py preprocess_2026_spot
uv run main.py preprocess_ukdata
```

### Start a Local HTTP Server

So that you can view it in your browser by go to `localhost:8000`

```bash
uv run python -m http.server -d outputs
```

If you have a hard time open the 2026 spot data visualization html, you can also run the gradio and open `localhost:12345`

```bash
uv run python src/gradio_2026_spot.py
```

## 2026 Update: Trends, Challenges, and Research

### 1. The Era of Gigawatt-Scale Volatility

The data center landscape has shifted to **gigawatt-scale campuses** (e.g., xAI's Colossus at 2 GW, Stargate targeting 4.5+ GW), but the critical new challenge is **volatility**. Unlike steady cloud workloads, AI training and inference create massive, rapid power fluctuations (ramping hundreds of MWs in sub-second intervals). Recent research characterizes how these power oscillations from large-scale AI workloads threaten local **grid frequency** and risk triggering cascading protection faults.

### 2. Critical Challenges: Interconnection & Quality

- **Interconnection Crisis & FERC Intervention:** With queues exceeding **8 years** in PJM, the **December 18, 2025 FERC Order** on Co-location has become a pivotal regulatory shift, allowing data centers to bypass traditional queues by siting behind-the-meter at power plants, despite "cost-shifting" debates.
- **Harmonics & Voltage Stability:** The massive deployment of non-linear loads (GPU rectifiers, VFD cooling) injects significant **harmonic distortion**. Recent studies show this degrades voltage stability for neighboring users. While data centers themselves aren't directly covered, co-located renewable energy and battery systems increasingly fall under **IEEE 2800-2022 standards** for inverter-based resources, which mandate improved grid integration capabilities including grid-forming operation.

### 3. Frontier Research: Data Centers as VPPs

Power system researchers are pivoting to treat data centers as active **Virtual Power Plants (VPPs)**:

- **Ancillary Services:** Beyond simple load shifting, data centers are integrating onsite Battery Energy Storage Systems (BESS) and leveraging thermal inertia to provide active **frequency regulation** and **voltage support**.
- **Co-Simulation:** New modeling frameworks are coupling power flow simulations with thermal/computational models to quantify the true flexibility of these assets, turning the "burden" of massive load into a tool for grid stability.

### 4. Deep Dive: Load Volatility & Mitigation

The "hundreds of MW" volatility is primarily a feature of **Large Model Training**, distinct from standard inference.

- **Training (The Volatile Load):**
  - **Mechanism:** Training involves synchronized "phases" across thousands of GPUs. When all GPUs finish a compute step and move to a communication step (all-reduce) or write a checkpoint to storage, their power consumption drops and spikes simultaneously.
  - **The Data:** Research confirms that training incurs large swings in power consumption **up to 37.5% of the provisioned power capacity within 2 seconds**, whereas inference only incurs changes of up to 9%. Extrapolating to a 50MW cluster, this represents approximately **18 MW swings in under 2 seconds** for training, but only **4 MW for inference**. Scaling to a 1GW campus (20x that size), a synchronized checkpoint or job restart could theoretically ramp **300-400 MW** in seconds.
  - **Checkpointing Spikes:** Writing a checkpoint to disk can cause GPUs to idle (power drop) and then immediately spike back to 100% utilization. For a 100,000 GPU cluster (approx. 70-100 MW of IT load alone), a synchronized restart is a massive grid event.
- **Inference (The Steady Load):**
  - **Mechanism:** Inference requests are typically stochastic (random) and independent. One user's query doesn't synchronize with another's.
  - **The Data:** For the same cluster size, inference fluctuations are significantly lower because the independent loads average out (9% vs 37.5% for training).

#### Industry Mitigation Strategies

Major tech companies are treating this as a physics/hardware problem:

- **Google (TPU Pods):** Uses **"Power Capping" & Fast Regulators**. Google employs software-defined power capping to limit the *rate of change* (di/dt) of power draw and specialized voltage regulator modules (VRMs) that can handle significant transient loads.
- **Meta (Grand Teton / Open Rack v3):** Deploys **Rack-Level Batteries (BBU)**. Meta's Open Rack v3 includes 48V battery backup units *directly on the rack*. These act as a "capacitive buffer" to smooth the demand seen by the utility grid.
- **Tesla/xAI (Colossus):** Integrates **Megapack Batteries**. Massive onsite Megapack batteries (168 units providing 150 MW storage) absorb the "shocks" from the GPU cluster's volatile load, presenting a smoother profile to the utility grid.

## Reference Materials

1. [Alibaba's Cluster Trace Program](https://github.com/alibaba/clusterdata)
2. [Data Centre Demand Profiles](https://ukpowernetworks.opendatasoft.com/explore/dataset/ukpn-data-centre-demand-profiles/information/?disjunctive.cleansed_voltage_level)
3. [World Data Center Map](https://www.datacentermap.com/)
4. [Paper: Data Center Model for Transient Stability Analysis of Power Systems](https://arxiv.org/abs/2505.16575)
5. [Report: Grid Flexibility Needs and Data Center Characteristics](https://www.epri.com/research/programs/063638/results/3002031504)
6. [Report: Data Center Flexibility: A Call to Action](./references/SIP_Data_Center_Flexibility_A_Call_to_Action.pdf)
7. [GPU Clusters Dataset](https://epoch.ai/data/gpu-clusters)
8. [Report: Power requirements of leading AI supercomputers have doubled every 13 months](https://epoch.ai/data-insights/ai-supercomputers-power-trend)
9. [Crusoe's Data Center (Over 3.4 GW)](https://www.crusoe.ai/data-centers)
10. [Turning AI Data Centers into Grid-Interactive Assets: Results from a Field Demonstration in Phoenix, Arizona](https://arxiv.org/abs/2507.00909)
11. [Characterisation and Quantification of Data Centre Flexibility for Power System Support](https://arxiv.org/abs/2511.07159)
12. [Grid Frequency Stability Support Potential of Data Center: A Quantitative Assessment of Flexibility](https://arxiv.org/abs/2510.01050)
13. [Datacenter Anatomy Part 1: Electrical Systems](https://newsletter.semianalysis.com/p/datacenter-anatomy-part-1-electrical)
14. [Datacenter Anatomy Part 2 – Cooling Systems](https://newsletter.semianalysis.com/p/datacenter-anatomy-part-2-cooling-systems)
15. [xAI's Colossus 2 - First Gigawatt Datacenter In The World, Unique RL Methodology, Capital Raise](https://newsletter.semianalysis.com/p/xais-colossus-2-first-gigawatt-datacenter)
16. [Datacenter](https://semianalysis.com/tag/datacenter/)
17. [AI Training Load Fluctuations at Gigawatt-scale - Risk of Power Grid Blackout?](https://newsletter.semianalysis.com/p/ai-training-load-fluctuations-at-gigawatt-scale-risk-of-power-grid-blackout)
18. [AI Infrastructure](https://semianalysis.com/tag/ai-infrastructure/)
19. [Colossus 1: xAI’s Ambitious 500 MW Energy Deployment](https://www.aterio.io/blog/colossus-1-xai-s-ambitious-500-mw-energy-deployment)
20. [Stargate advances with 4.5 GW partnership with Oracle](https://openai.com/index/stargate-advances-with-partnership-with-oracle/)
21. [Stargate Community](https://openai.com/index/stargate-community/)
    1. [OpenAI, Oracle and Vantage Data Centers Announce Stargate Data Center Site in Wisconsin](https://vantage-dc.com/news/openai-oracle-and-vantage-data-centers-announce-stargate-data-center-site-in-wisconsin/)
    2. [Saline Township (Stargate + DTE in Michigan)](https://www.related-digital.com/michigan)
    3. [OpenAI and SoftBank Group partner with SB Energy](https://openai.com/index/stargate-sb-energy-partnership/)
22. [Amazon plans to invest at least $3 billion in Warren County, Mississippi, for next-generation data center campus](https://www.aboutamazon.com/news/company-news/amazon-3-billion-mississippi-data-center-investment) (Five renewable energy projects generating 616 MWs—equivalent to powering 152,000 U.S. homes)
23. [Made in Wisconsin: The world’s most powerful AI datacenter](https://blogs.microsoft.com/on-the-issues/2025/09/18/made-in-wisconsin-the-worlds-most-powerful-ai-datacenter/)
24. [Blog: xAI Colossus Hits 2 GW: 555,000 GPUs, $18B, Largest AI Site](https://introl.com/blog/xai-colossus-2-gigawatt-expansion-555k-gpus-january-2026#:~:text=TL;DR,benchmark%20for%20scale%20and%20speed.)
25. [Data Center Power Consumption by State (2025)](https://www.electricchoice.com/blog/datacenters-electricity/)
26. [Infinite scale: The architecture behind the Azure AI superfactory](https://blogs.microsoft.com/blog/2025/11/12/infinite-scale-the-architecture-behind-the-azure-ai-superfactory/)
27. [How Amazon is harnessing solar energy, batteries, and AI to help decarbonize the grid](https://www.aboutamazon.com/news/sustainability/carbon-free-energy-projects-ai-tech)
28. [Amazon signs agreements for innovative nuclear energy projects to address growing energy demands](https://www.aboutamazon.com/news/sustainability/amazon-nuclear-small-modular-reactor-net-carbon-zero)
29. [AWS activates Project Rainier: One of the world’s largest AI compute clusters comes online](https://www.aboutamazon.com/news/aws/aws-project-rainier-ai-trainium-chips-compute-cluster)
30. [Amazon opens $11b AI data center in Indiana](https://www.techinasia.com/news/amazon-opens-11b-ai-data-center-indiana)
31. [Meta’s Dual-Track Data Center Strategy: Owning AI Campuses, Leasing Cloud, and Expanding Nationwide](https://www.datacenterfrontier.com/hyperscale/article/55310441/ownership-and-power-challenges-in-metas-hyperion-and-prometheus-data-centers)
32. [Meet Prometheus: World’s highest capacity data center slated to open in Ohio in 2026](https://www.nbc4i.com/news/local-news/new-albany/meet-prometheus-worlds-highest-capacity-data-center-slated-to-open-in-ohio-in-2026/)
33. [Google Data Centers Operating Sustainably: Water Stewardship, Energy Solutions, and Power Efficiency](https://datacenters.google/operating-sustainably/)
34. [Google: How we’re making data centers more flexible to benefit power grids](https://blog.google/innovation-and-ai/infrastructure-and-cloud/global-network/how-were-making-data-centers-more-flexible-to-benefit-power-grids/)
35. [Columbus will become second-largest data center hub in the Great Lakes region, report says](https://www.wosu.org/2026-01-19/columbus-will-become-second-largest-data-center-hub-in-the-great-lakes-region-report-says)
36. [SemiAnalysis: Top 10 largest AI Datacenters in 2026](https://youtu.be/a-9egkpaZUw?si=r363pQ2sAMlRlTbl)
37. [FACT SHEET | FERC Directs Nation’s Largest Grid Operator to Create New Rules to Embrace Innovation and Protect Consumers](https://www.ferc.gov/news-events/news/fact-sheet-ferc-directs-nations-largest-grid-operator-create-new-rules-embrace)
38. [Data center grid-power demand to rise 22% in 2025, nearly triple by 2030](https://www.spglobal.com/energy/en/news-research/latest-news/electric-power/101425-data-center-grid-power-demand-to-rise-22-in-2025-nearly-triple-by-2030)
39. [Senate Democrats propose legislation to curb data center grid impact](https://www.datacenterdynamics.com/en/news/senate-democrats-propose-legislation-to-curb-data-center-grid-impact/)
40. [Electricity Demand and Grid Impacts of AI Data Centers: Challenges and Prospects](https://arxiv.org/abs/2509.07218v4)
41. [Managing Harmonic Distortion from Data Centers](https://www.dynamicratings.com/managing-harmonic-distortion-from-data-centers/)
42. [Galaxy Completes ERCOT Interconnection Studies and Secures Approval for Additional 830 Megawatts at Helios Data Center Campus, Doubling Total Approved Power Capacity to over 1.6 Gigawatts](https://investor.galaxy.com/news-releases/news-release-details/galaxy-completes-ercot-interconnection-studies-and-secures)
43. [training vs inference](https://www.glennklockwood.com/garden/training-vs-inference)
44. [Characterizing Power Management Opportunities for LLMs in the Cloud](https://dl.acm.org/doi/epdf/10.1145/3620666.3651329)
45. [Analog Devices: Impacts of Transients on AI Accelerator Card Power Delivery](https://www.analog.com/en/resources/technical-articles/impacts-of-transients-on-ai-accelerator-card-power-delivery.html)
46. [arXiv: TPU v4: An Optically Reconfigurable Supercomputer for Machine Learning](https://arxiv.org/abs/2304.01433)
47. [Meta Open Rack V3 BBU Shelf Specification](https://www.opencompute.org/documents/open-rack-v3-bbu-shelf-spec-rev1-1-pdf-1)
48. [Open Compute Project: Rack & Power](https://www.opencompute.org/projects/rack-and-power)
49. [CNBC: Tesla sold $430M worth of Megapacks to xAI in 2025](https://www.cnbc.com/2026/01/29/tesla-sold-430-million-worth-of-its-megapack-batteries-to-xai-in-2025.html)
50. [DCD: xAI deploys 168 Tesla Megapacks](https://www.datacenterdynamics.com/en/news/xai-deploys-168-tesla-megapacks-to-power-its-colossus-supercomputer-in-memphis/)
51. [arXiv: Wide-Area Power System Oscillations from Large-Scale AI Workloads](https://arxiv.org/abs/2508.16457)
52. [RMI: PJM's Speed to Power Problem](https://rmi.org/pjms-speed-to-power-problem-and-how-to-fix-it/)
53. [IEEE 2800 Standard: How It Impacts IBR Interconnection](https://www.zeroemissiongrid.com/insights-press-zeg-blog/ieee-2800-standard-how-it-impacts-ibr-interconnection-and-what-developers-must-know/)
