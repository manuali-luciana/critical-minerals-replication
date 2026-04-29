# CriMinGeo — Critical Minerals Supply Chain Dashboard

Descriptive panorama of global critical mineral supply chains (mining, refining, trade), 2000–2023, for the CriMinGeo project (IAE-CSIC, PI Christopher Rauh).

This is **v1** — functional, not yet polished. Built as a static site so it can be hosted free on GitHub Pages with no server, no maintenance, and no API keys.

---

## What this is

A single-page interactive dashboard with four views:

- **Overview** — a 13-mineral × 2-stage matrix showing the leading country and concentration metric (Top-1 share or HHI) in the most recent year of available data. Click a row to drill in.
- **Mineral profile** — for any selected mineral, stacked-area time series for mining and refining production, year-aware concentration statistics, and trade flows (top exporters/importers, primary vs refined).
- **Compare minerals** — pick up to 4 minerals, see Top-1 share and HHI side-by-side over time for both stages.
- **Methodology drawer** — context-aware notes on sources, units, harmonization, caveats. Always one click away.

Time window is fixed at 2000–2023. The full BGS series back to 1971 is preserved in `data/source/`; only the dashboard view is windowed.

---

## Repository layout

```
critical-minerals-replication/
├── code/
│   ├── build_dashboard_data.py    ← prep script (you run this)
│   └── ... (your existing code)
├── data/
│   └── source/                    ← raw BGS, USGS, UN Comtrade CSVs
├── docs/                          ← what GitHub Pages serves
│   ├── index.html                 ← the dashboard (single file)
│   └── data/                      ← generated JSONs (one per mineral)
└── README.md
```

The `docs/` folder name is a GitHub Pages convention. When you turn on Pages and select "from /docs", it serves whatever's in there.

---

## How to build it (first time)

You need Python 3 with `pandas` and `numpy`. If you don't have them, install once:

```bash
pip install pandas numpy
```

Then, from the project root:

```bash
python code/build_dashboard_data.py
```

This reads every CSV in `data/source/`, harmonizes country names, separates mining from refining where the source distinguishes them, computes concentration metrics, and writes out one JSON per mineral plus an overview file into `docs/data/`.

The script is **idempotent** — running it again just overwrites the JSONs. It's fine to run it any time you change the source data.

If a CSV file is missing, the script logs a `[skip]` line and produces an empty JSON for that mineral. The dashboard handles this gracefully: missing minerals show as "No data in v1" cells in the overview, and clicking them shows an explanation panel.

---

## How to view it locally (before pushing)

You can't just double-click `docs/index.html` — browsers block local-file `fetch()` calls for security. You need a tiny local web server:

```bash
cd docs
python -m http.server 8000
```

Then open <http://localhost:8000> in your browser. Stop the server with `Ctrl+C`.

---

## How to deploy to GitHub Pages (one-time setup)

After committing and pushing the build:

1. Go to your repo on GitHub: <https://github.com/manuali-luciana/critical-minerals-replication>
2. Click **Settings** (top right of repo page)
3. Click **Pages** in the left sidebar
4. Under "Build and deployment":
   - **Source**: "Deploy from a branch"
   - **Branch**: `main` · **Folder**: `/docs`
5. Click **Save**

Within ~60 seconds, GitHub will publish your dashboard at:

> **<https://manuali-luciana.github.io/critical-minerals-replication/>**

Each subsequent push to `main` that touches `docs/` triggers an automatic redeploy.

---

## How to update when new data lands

Two scenarios.

**Scenario A: new year of an existing source.** USGS releases MCS 2026, or a new BGS year is published, or you re-run the Comtrade download with 2024 added.

```bash
# 1. Drop the new CSV into data/source/, replacing the old file
# 2. Rebuild the JSONs
python code/build_dashboard_data.py
# 3. Commit and push
git add data/source docs/data
git commit -m "Update data: 2024"
git push
```

If you're extending the window to 2024, also edit the top of `code/build_dashboard_data.py`:

```python
YEAR_MAX = 2024  # was 2023
```

…and update the static text in `docs/index.html` (search for "2000–2023").

**Scenario B: a brand-new mineral file.** Add the file path to the `MINERALS` dict in `build_dashboard_data.py`, then rebuild.

---

## Known limitations of v1

These are documented in the dashboard's methodology drawer too — keeping them visible matters more than fixing them all on day one.

- **Country-name harmonization** covers the top ~25 producers per mineral cleanly. The long tail may show small inconsistencies between BGS, USGS, and Comtrade naming conventions. Iterative.
- **Lithium and rare-earth units** are presented as BGS reports them (gross tonnes, summed across sub-commodities). Conversion to elemental content (Li metal, REO equivalent) is a research decision pending Christopher's input.
- **Trade data and HS codes**: HS codes are an imperfect proxy for the mining/refining distinction. The dashboard surfaces the HS codes used per stage in the methodology drawer; some flows that look surprising are HS-code artifacts (e.g. DRC's "refined" cobalt exports are largely cobalt hydroxide concentrate).
- **Reserves data** is in the source files but not yet integrated into the dashboard. A v1.5 task.
- **Event annotations** on the time series (e.g. 2010 China-Japan REE dispute, 2014 Indonesia ore ban) are deferred to v2 pending a curated event list.
- **Refining data is genuinely missing for several minerals.** This is not a build bug — it reflects the underlying public sources. The dashboard shows these explicitly as "No data in v1" cells rather than hiding them.

---

## Architecture rationale

- **Static site over Streamlit**: free hosting forever, no "app went to sleep" problems when the PI is presenting at the European Commission, instant interactions because everything is client-side.
- **Vanilla JS + d3** rather than React: zero build step, single file you can read end-to-end, no node_modules.
- **JSON files per mineral** rather than one big file: lazy loading keeps initial paint fast, makes diffs reviewable when data changes.
- **`docs/` folder**: GitHub Pages convention; keeps generated artifacts visibly separate from source code.

---

## Build info

- Built: see footer of dashboard
- PI: Christopher Rauh, IAE-CSIC
- Project: Critical Minerals and Geoeconomics (CriMinGeo), 2024 Proyectos de Generación de Conocimiento
- Data sources: British Geological Survey · USGS Mineral Commodity Summaries · UN Comtrade
