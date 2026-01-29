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