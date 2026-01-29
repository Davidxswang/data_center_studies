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

## Reference Materials

1. [Alibaba's Cluster Trace Program](https://github.com/alibaba/clusterdata)
2. [Data Centre Demand Profiles](https://ukpowernetworks.opendatasoft.com/explore/dataset/ukpn-data-centre-demand-profiles/information/?disjunctive.cleansed_voltage_level)
3. [World Data Center Map](https://www.datacentermap.com/)
4. [Paper: Data Center Model for Transient Stability Analysis of Power Systems](https://arxiv.org/abs/2505.16575)
5. [Report: Grid Flexibility Needs and Data Center Characteristics](https://www.epri.com/research/programs/063638/results/3002031504)
6. [Report: Data Center Flexibility: A Call to Action](./references/SIP_Data_Center_Flexibility_A_Call_to_Action.pdf)
7. [GPU Clusters Dataset](https://epoch.ai/data/gpu-clusters)
8. [Report: Power requirements of leading AI supercomputers have doubled every 13 months](https://epoch.ai/data-insights/ai-supercomputers-power-trend)
9. [Blog: xAI Colossus Hits 2 GW: 555,000 GPUs, $18B, Largest AI Site](https://introl.com/blog/xai-colossus-2-gigawatt-expansion-555k-gpus-january-2026#:~:text=TL;DR,benchmark%20for%20scale%20and%20speed.)
10. [Crusoe's Data Center (Over 3.4 GW)](https://www.crusoe.ai/data-centers)
11. [Turning AI Data Centers into Grid-Interactive Assets: Results from a Field Demonstration in Phoenix, Arizona](https://arxiv.org/abs/2507.00909)
12. [Characterisation and Quantification of Data Centre Flexibility for Power System Support](https://arxiv.org/abs/2511.07159)
13. [Grid Frequency Stability Support Potential of Data Center: A Quantitative Assessment of Flexibility](https://arxiv.org/abs/2510.01050)
14. [Datacenter Anatomy Part 1: Electrical Systems](https://newsletter.semianalysis.com/p/datacenter-anatomy-part-1-electrical)
15. [Datacenter Anatomy Part 2 – Cooling Systems](https://newsletter.semianalysis.com/p/datacenter-anatomy-part-2-cooling-systems)
16. [xAI's Colossus 2 - First Gigawatt Datacenter In The World, Unique RL Methodology, Capital Raise](https://newsletter.semianalysis.com/p/xais-colossus-2-first-gigawatt-datacenter)
17. [Datacenter](https://semianalysis.com/tag/datacenter/)
18. [AI Training Load Fluctuations at Gigawatt-scale - Risk of Power Grid Blackout?](https://newsletter.semianalysis.com/p/ai-training-load-fluctuations-at-gigawatt-scale-risk-of-power-grid-blackout)
19. [AI Infrastructure](https://semianalysis.com/tag/ai-infrastructure/)