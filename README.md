# labourAIVolt: AI & Human Labour Displacement Analysis for Volt

[![LAV Analysis](https://github.com/CGutt-hub/labourAIVolt/actions/workflows/lav_analysis.yml/badge.svg)](https://github.com/CGutt-hub/labourAIVolt/actions/workflows/lav_analysis.yml)

This repository hosts a **Nextflow + Python analysis pipeline** that automatically
fetches current labour-market data from the
[World Bank public API](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392)
and quantifies **AI-driven labour displacement** across the six Volt EU countries with
the most active chapters — Germany, France, the Netherlands, Belgium, Italy, and Spain.

The aim is to give [Volt Europa](https://www.volteuropa.org/) and its national chapters
an evidence base for labour-market and technology policy: *which sectors are shedding jobs
fastest as AI and automation accelerate, which countries are most exposed, and how does
digital readiness moderate that exposure?*

The pipeline is architecturally modelled after the
**[EmotiView](https://github.com/CGutt-hub/EmotiView)** project and extends the
**[AnalysisToolbox](https://github.com/CGutt-hub/AnalysisToolbox)** modular Nextflow
framework for scalable, reproducible analysis with automatic result synchronisation.

| Platform | Role | Contents |
|----------|------|----------|
| **[GitHub](https://github.com/CGutt-hub/labourAIVolt)** | Technical implementation | Pipeline, scripts, results |
| **World Bank API** | Data source | Live labour-market indicators |

---

## Research Background

The labour-market impact of AI and automation is one of the defining policy challenges
of the 2020s. Early projections (Frey & Osborne, 2013) estimated that up to 47 % of US
jobs faced high computerisation risk; subsequent analyses have moderated that figure while
broadening it to task-level disruption rather than wholesale job destruction. What is clear
is that the *pace* and *sector distribution* of displacement vary substantially across
countries depending on industrial structure, education levels, and digital infrastructure.

For a pan-European political movement like Volt, the relevant questions are:

1. **Which EU sectors show the clearest employment decline correlated with automation?**
2. **Are Volt's home countries converging toward or diverging from each other on displacement pressure?**
3. **Does a country's digital readiness (internet penetration, high-tech exports) buffer it against displacement?**
4. **Where should Volt's labour policy — reskilling funds, working-time reform, Universal Basic Income pilots — be concentrated first?**

This pipeline operationalises those questions with reproducible, automatically updated data.

---

## Core Research Questions & Hypotheses

1. **Sector displacement ordering**: Industry and agriculture will show larger negative
   employment-share trends than services, consistent with higher Frey & Osborne automation
   risk for routine physical/cognitive tasks.

2. **Cross-country heterogeneity**: Countries with larger manufacturing sectors (Germany,
   Italy) will exhibit higher AI Displacement Pressure Index (ADPI) than service-dominant
   economies (Netherlands, Belgium).

3. **Digitalization buffer**: Countries with higher Digitalization Readiness Scores
   (internet penetration + high-tech export share) will show lower net Vulnerability Scores,
   suggesting that digital transformation simultaneously creates displacement *and* provides
   adaptive capacity.

4. **Temporal acceleration**: Employment-share trends will steepen post-2018 as AI adoption
   accelerates across all three sectors, visible as a structural break in the time-series.

---

## Analysis Pipeline

The pipeline is built on the
**[AnalysisToolbox](https://github.com/CGutt-hub/AnalysisToolbox)** — a modular Nextflow
framework for scalable, reproducible data processing with automatic result synchronisation.
The LAV-specific pipeline in `LAV_analysis/` extends this framework to:

*   Discover country datasets and create per-country output directories (L1).
*   Fetch and parse live labour-market time-series from the World Bank API.
*   Perform standardised normalisation and cleaning.
*   Extract displacement signals and automation-risk-weighted scores per sector.
*   Fit time-series trend models to all available indicators.
*   Aggregate across countries into cross-country rankings and Volt policy metrics (L2).

Configuration is managed via `LAV_analysis/LAV_parameters.config`.
See the [AnalysisToolbox documentation](https://github.com/CGutt-hub/AnalysisToolbox)
for framework details.

### Pipeline steps

```
L1 (per country)
┌─────────────────────────────────────────────────────────────────────┐
│  api_reader              Fetch 13 World Bank indicators (2010–2023) │
│       ↓                                                             │
│  normalizing_processor   Pivot long→wide; sort; deduplicate         │
│       ↓              ↘                                              │
│  displacement_analyzer   trend_analyzer                             │
│  sector scores ×         OLS slope + p-value + R²                  │
│  Frey & Osborne risk     per indicator                              │
└─────────────────────────────────────────────────────────────────────┘
       ↓ collect all countries
L2 (cross-country)
┌─────────────────────────────────────────────────────────────────────┐
│  volt_report_analyzer    Displacement ranking · ADPI · DRS          │
│                          Vulnerability Score · policy metrics       │
└─────────────────────────────────────────────────────────────────────┘
```

### Displacement model

Each broad employment sector receives a **displacement score**:

```
displacement_score  =  displacement_signal  ×  automation_risk
```

| Term | Definition |
|------|-----------|
| `displacement_signal` | Normalised negative employment-share trend: `max(0, −slope / mean_level)`. A sector losing share faster relative to its baseline scores higher. |
| `automation_risk` | Sector-level probability of computerisation from Frey & Osborne (2013): agriculture 0.82, industry 0.79, services 0.63. |
| `displacement_score` | Composite: high score = fast employment decline *and* high intrinsic automation susceptibility. |

### Group-level metrics (L2)

| Metric | Definition |
|--------|-----------|
| **ADPI** (AI Displacement Pressure Index) | Mean `displacement_score` across all sectors for a country. |
| **DRS** (Digitalization Readiness Score) | Normalised mean of internet-user percentage and high-tech export share (latest year). |
| **Vulnerability Score** | `ADPI / (DRS + ε)` — high ADPI *and* low digital readiness = most vulnerable. |

### Data indicators (World Bank API, no key required)

| Column | World Bank code | Description |
|--------|----------------|-------------|
| `employment_agriculture_pct` | SL.AGR.EMPL.ZS | Employment in agriculture (% total) |
| `employment_industry_pct` | SL.IND.EMPL.ZS | Employment in industry (% total) |
| `employment_services_pct` | SL.SRV.EMPL.ZS | Employment in services (% total) |
| `unemployment_rate` | SL.UEM.TOTL.ZS | Unemployment (% labour force) |
| `youth_unemployment_rate` | SL.UEM.1524.ZS | Youth unemployment (%) |
| `employment_to_pop_ratio` | SL.EMP.TOTL.SP.ZS | Employment-to-population ratio |
| `wage_salary_workers_pct` | SL.EMP.WORK.ZS | Wage & salaried workers (%) |
| `internet_users_pct` | IT.NET.USER.ZS | Internet users (% population) |
| `gdp_per_capita_usd` | NY.GDP.PCAP.CD | GDP per capita (current USD) |
| `gdp_growth_annual_pct` | NY.GDP.MKTP.KD.ZG | GDP growth (annual %) |
| `high_tech_exports_pct_mfg` | TX.VAL.TECH.MF.ZS | High-tech exports (% manufactured exports) |
| `ict_goods_exports_pct` | TX.VAL.ICTG.ZS.UN | ICT goods exports (% total goods exports) |
| `labor_force_total` | SL.TLF.TOTL.IN | Total labour force |

---

## Repository Structure

```
labourAIVolt/
│
├── LAV_analysis/                    Nextflow pipeline (mirrors EV_analysis/)
│   ├── LAV_pipeline.nf              Main workflow orchestration
│   ├── LAV_modules.nf               IOInterface alias declarations
│   └── LAV_parameters.config        All pipeline parameters & script paths
│
├── LAV_data/                        Per-country input configs (mirrors rawData/)
│   ├── LAV_001/  LAV_001_config.json    Germany        (Volt Deutschland)
│   ├── LAV_002/  LAV_002_config.json    France         (Volt France)
│   ├── LAV_003/  LAV_003_config.json    Netherlands    (Volt Nederland)
│   ├── LAV_004/  LAV_004_config.json    Belgium        (Volt Belgium)
│   ├── LAV_005/  LAV_005_config.json    Italy          (Volt Italia)
│   └── LAV_006/  LAV_006_config.json    Spain          (Volt España)
│
├── LAV_results/                     Pipeline outputs (mirrors EV_results/)
│   ├── .bin/                        Shared infrastructure (logs, HTML archive)
│   ├── LAV_l1/                      First-level: per-country results
│   │   ├── LAV_001/
│   │   │   ├── plots/               Parquet output copies for QC
│   │   │   ├── LAV_001_api_raw.parquet
│   │   │   ├── LAV_001_normalized.parquet
│   │   │   ├── LAV_001_displacement.parquet
│   │   │   ├── LAV_001_trends.parquet
│   │   │   └── LAV_001.log.parquet  Live execution log
│   │   └── LAV_002/ … LAV_006/
│   └── LAV_l2/                      Second-level: cross-country group results
│       ├── LAV_volt_report.parquet
│       ├── LAV_displacement_summary.parquet
│       └── LAV_trends_summary.parquet
│
├── Python/                          Analysis scripts (no Nextflow dependency)
│   ├── lav_run.py                   Standalone orchestrator (used by CI)
│   ├── requirements.txt
│   ├── readers/
│   │   └── api_reader.py            Fetches World Bank labour-market data
│   ├── processors/
│   │   └── normalizing_processor.py Long→wide pivot, clean, sort
│   └── analyzers/
│       ├── displacement_analyzer.py AI displacement scores (Frey & Osborne)
│       ├── trend_analyzer.py        OLS time-series trends per indicator
│       └── volt_report_analyzer.py  Cross-country Volt policy synthesis
│
└── .github/workflows/
    └── lav_analysis.yml             GitHub Actions CI (weekly + on push)
```

---

## Running the Analysis

### Option A — GitHub Actions *(recommended — no local setup needed)*

The workflow in `.github/workflows/lav_analysis.yml` runs automatically:

| Trigger | When |
|---------|------|
| **Scheduled** | Every Monday at 06:00 UTC (pulls the latest World Bank data) |
| **On push** | Any change to `LAV_data/**` or `Python/**` on `main` |
| **Manual** | Actions tab → *LAV Labour-AI-Volt Analysis* → **Run workflow** |

Results are:
1. Uploaded as a **downloadable artifact** (`lav-results-<run-number>`) for 90 days.
2. **Committed back** to `LAV_results/` in the repository so outputs are versioned
   alongside the code.

No API keys, secrets, or local software are required.

---

### Option B — Standalone Python *(local, no Nextflow)*

Use this for quick local runs or debugging individual scripts.

```bash
# 1. Clone the repository
git clone https://github.com/CGutt-hub/labourAIVolt.git
cd labourAIVolt

# 2. Install Python dependencies
pip install -r Python/requirements.txt

# 3. Run the full pipeline
python Python/lav_run.py

# Optional: override data/output directories
python Python/lav_run.py --data-dir LAV_data --output-dir LAV_results
```

Results are written to `LAV_results/LAV_l1/<id>/` (per country) and
`LAV_results/LAV_l2/` (group synthesis).

---

### Option C — Full Nextflow Pipeline *(local, requires AnalysisToolbox)*

Use this for full pipeline tracing, parallel execution, and integration with the
AnalysisToolbox interactive HTML archive.

**Prerequisites:** Java ≥ 11, [Nextflow](https://www.nextflow.io/docs/latest/install.html)

```bash
# 1. Clone both repos as siblings
git clone https://github.com/CGutt-hub/labourAIVolt.git
git clone https://github.com/CGutt-hub/AnalysisToolbox.git

# Your directory should now look like:
#   parent/
#   ├── AnalysisToolbox/
#   └── labourAIVolt/

# 2. Install Python dependencies
cd labourAIVolt
pip install -r Python/requirements.txt

# 3. Adjust python_exe in LAV_parameters.config if needed
#    (default: 'python3')

# 4. Launch the pipeline from the LAV_analysis/ directory
cd LAV_analysis
nextflow run LAV_pipeline.nf -c LAV_parameters.config
```

The Nextflow pipeline adds on top of the standalone runner:
- Parallel per-country execution
- Full Nextflow trace (`LAV_results/.bin/pipeline_trace.txt`)
- Interactive HTML result archive (via AnalysisToolbox `interactive_plotter`)
- Automatic git commit + push of results after each country completes

---

## Output Files Reference

### Per-country (L1) — `LAV_results/LAV_l1/LAV_XXX/`

| File | Description |
|------|-------------|
| `LAV_XXX_api_raw.parquet` | Raw long-format data as returned by the World Bank API. Columns: `participant_id`, `country`, `iso3`, `source`, `indicator`, `indicator_code`, `year`, `value`. |
| `LAV_XXX_normalized.parquet` | Wide-format time-series. One row per year, one column per indicator. Ready for analysis scripts. |
| `LAV_XXX_displacement.parquet` | Per-sector displacement scores. Key columns: `sector`, `employment_mean_pct`, `trend_slope_pp_per_yr`, `trend_significant`, `automation_risk_frey_osborne`, `displacement_score`. |
| `LAV_XXX_trends.parquet` | OLS trend results for every indicator. Key columns: `indicator`, `trend_slope`, `trend_p_value`, `trend_r_squared`, `trend_significant`, `total_change_pct`. |
| `LAV_XXX.log.parquet` | Live pipeline execution log (Nextflow mode only). |

### Group-level (L2) — `LAV_results/LAV_l2/`

| File | Description |
|------|-------------|
| `LAV_volt_report.parquet` | Full combined table (displacement + policy metrics for all countries). |
| `LAV_displacement_summary.parquet` | Cross-country displacement ranking per sector, with EU-wide mean, std, and per-country rank. |
| `LAV_trends_summary.parquet` | EU-wide mean slope and significance counts for key indicators across all countries. |

---

## Adding a New Country

1. Create a new directory: `LAV_data/LAV_007/`
2. Add a config file `LAV_data/LAV_007/LAV_007_config.json`:

```json
{
  "participant_id": "LAV_007",
  "country": "Portugal",
  "iso3": "PRT",
  "iso2": "PT",
  "year_start": 2010,
  "year_end": 2025,
  "volt_chapter": "Volt Portugal",
  "population_millions": 10.3,
  "eu_member": true,
  "notes": "Optional notes about the country context"
}
```

3. Push the file — the GitHub Action will pick it up automatically on the next run.

---

## Project Status

Active development. Data fetching, pipeline, and group analysis are operational.
Planned additions: visualisation layer, structural-break detection (2018 AI inflection
point), and integration with OECD employment-by-occupation microdata for finer-grained
occupational risk scoring.

---

## References

*   Frey, C. B., & Osborne, M. A. (2013). *The Future of Employment: How Susceptible Are
    Jobs to Computerisation?* Oxford Martin School Working Paper.
*   World Bank Open Data. [https://data.worldbank.org](https://data.worldbank.org)
*   Acemoglu, D., & Restrepo, P. (2020). Robots and Employment: Evidence from Europe.
    *American Economic Review*, 110(6), 2188–2220.
*   Autor, D. (2015). Why Are There Still So Many Jobs? *Journal of Economic Perspectives*,
    29(3), 3–30.

---

## Contributors

| Name | Role | Contact |
|------|------|---------|
| **Cagatay Özcan Jagiello Gutt** | Principal Investigator | [![ORCID](https://img.shields.io/badge/ORCID-0000--0002--1774--532X-green?logo=orcid)](https://orcid.org/0000-0002-1774-532X) |
