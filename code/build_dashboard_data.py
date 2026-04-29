"""
build_dashboard_data.py

Reads raw BGS, USGS MCS, and UN Comtrade CSVs from data/source/ and produces
small JSON files in docs/data/ for the static dashboard.

Run from project root:
    python code/build_dashboard_data.py

Output: docs/data/manifest.json + one JSON per mineral.

Design choices documented in METHODOLOGY notes (see docs/index.html drawer):
- Window: 2000-2023 (aligned with UN Comtrade availability)
- BGS mining/refining split: filtered on bgs_commodity_trans
- Country names: harmonized via COUNTRY_NAME_MAP below
- Lithium: aggregated across all sub-commodities reported by BGS (gross tonnes
  of mineral; NOT Li content). Methodology drawer flags this.
- REEs: aggregated across all sub-commodities (REO equivalent / gross tonnes).
- Trade: bilateral flows only (partnerISO != 'W00'); split by 'stage' column
  ('primary' vs 'refined') as set by the upstream Comtrade download script.
"""

import json
import os
from pathlib import Path
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

YEAR_MIN = 2000
YEAR_MAX = 2023

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = PROJECT_ROOT / "data" / "source"
OUTPUT_DIR = PROJECT_ROOT / "docs" / "data"

# 13 minerals + the BGS file basename(s) that contain them.
# Some BGS files contain multiple stages (e.g. cobalt has both mine + refined rows).
MINERALS = {
    "antimony":   {"bgs": ["BGS_antimony_production.csv"],   "trade": "UN_antimony_trade.csv",   "label": "Antimony"},
    "cobalt":     {"bgs": ["BGS_cobalt_production.csv"],     "trade": "UN_cobalt_trade.csv",     "label": "Cobalt"},
    "copper":     {"bgs": ["BGS_copper_production.csv"],     "trade": "UN_copper_trade.csv",     "label": "Copper"},
    "gallium":    {"bgs": ["BGS_gallium_production.csv"],    "trade": "UN_gallium_trade.csv",    "label": "Gallium"},
    "germanium":  {"bgs": ["BGS_germanium_production.csv"],  "trade": "UN_germanium_trade.csv",  "label": "Germanium"},
    "graphite":   {"bgs": ["BGS_graphite_production.csv"],   "trade": "UN_graphite_trade.csv",   "label": "Natural Graphite"},
    "lithium":    {"bgs": ["BGS_lithium_production.csv"],    "trade": "UN_lithium_trade.csv",    "label": "Lithium"},
    "magnesium":  {"bgs": ["BGS_magnesium_production.csv"],  "trade": "UN_magnesium_trade.csv",  "label": "Magnesium"},
    "manganese":  {"bgs": ["BGS_manganese_production.csv", "BGS_ferroalloys_production.csv"],
                                                              "trade": "UN_manganese_trade.csv",  "label": "Manganese"},
    "nickel":     {"bgs": ["BGS_nickel_production.csv"],     "trade": "UN_nickel_trade.csv",     "label": "Nickel"},
    "pgms":       {"bgs": ["BGS_platinum_production.csv"],   "trade": "UN_pgm_trade.csv",        "label": "PGMs"},
    "ree":        {"bgs": ["BGS_REs_production.csv"],        "trade": "UN_ree_trade.csv",        "label": "Rare Earths"},
    "tungsten":   {"bgs": ["BGS_tungsten_production.csv"],   "trade": "UN_tungsten_trade.csv",   "label": "Tungsten"},
}

# Map stage labels in BGS bgs_commodity_trans to our two canonical stages.
# IMPORTANT: 'mine' is checked before 'metal' so labels like "platinum group
# metals, mine" classify correctly. Order within each list doesn't matter
# but we check the mining list first overall.
STAGE_PATTERNS = {
    "mining": ["mine", "ore", "concentrate", "minerals", "oxides"],
    "refining": ["refined", "smelter", "ferroalloy", "ferro-alloy", "ferromanganese", "silicomanganese", "metal", "primary"],
}

# Per-mineral overrides for cases where the BGS labels don't fit the standard
# classifier or where we want to drop a stage entirely.
#
# Each override is one of:
#   ("force_stage", "mining"|"refining")  - all rows treated as that stage
#   ("drop_stage",  "mining"|"refining")  - all rows of that stage are removed
#   ("force_all",   "mining"|"refining")  - all rows from this file forced to one stage
#
# Documented rationale for each case in the notes drawer of the dashboard.
MINERAL_STAGE_OVERRIDES = {
    # Gallium: byproduct of bauxite-to-alumina and zinc metallurgy. No mining
    # stage exists. The "gallium, primary" figure is the first country-level
    # production datum and represents refining/recovery.
    "gallium": ("force_all", "refining"),

    # Germanium: byproduct of zinc mining and coal ash. No mining stage exists.
    # The "germanium metal" figure is the first country-level production datum.
    "germanium": ("force_all", "refining"),

    # Magnesium: extracted from dolomite/magnesite/brines but the upstream
    # tonnage attributable to Mg metal production is not reported. The
    # "magnesium metal, primary" figure is the refining/extraction stage.
    "magnesium": ("force_all", "refining"),

    # PGMs: BGS reports "platinum group metals, mine" and refined figures
    # that are essentially the same numbers attributed to the same countries.
    # To avoid implying a separate refining stage exists, we show only mining.
    "pgms": ("drop_stage", "refining"),

    # Graphite: BGS uses a single label "graphite" (no "mine"/"ore" suffix).
    # Natural graphite is extracted directly; the single label IS the mining
    # stage. Refined / spherical graphite (battery anode material) is not
    # reported by country in public BGS series.
    "graphite": ("force_all", "mining"),
}

# Country name harmonization. Maps various spellings to a canonical short name + ISO3.
# Keep canonical names short; the dashboard renders them in tight chart legends.
COUNTRY_HARMONIZATION = {
    # name in source -> (canonical_name, iso3)
    "China": ("China", "CHN"),
    "China, mainland": ("China", "CHN"),
    "Congo, Democratic Republic": ("DRC", "COD"),
    "Dem. Rep. of the Congo": ("DRC", "COD"),
    "Congo (Kinshasa)": ("DRC", "COD"),
    "Democratic Republic of the Congo": ("DRC", "COD"),
    "Australia": ("Australia", "AUS"),
    "Chile": ("Chile", "CHL"),
    "Indonesia": ("Indonesia", "IDN"),
    "South Africa": ("South Africa", "ZAF"),
    "Russia": ("Russia", "RUS"),
    "Russian Federation": ("Russia", "RUS"),
    "Soviet Union": ("USSR (former)", "SUN"),
    "Myanmar": ("Myanmar", "MMR"),
    "Burma": ("Myanmar", "MMR"),
    "USA": ("United States", "USA"),
    "United States": ("United States", "USA"),
    "United States of America": ("United States", "USA"),
    "Brazil": ("Brazil", "BRA"),
    "Philippines": ("Philippines", "PHL"),
    "Canada": ("Canada", "CAN"),
    "Tajikistan": ("Tajikistan", "TJK"),
    "Japan": ("Japan", "JPN"),
    "Peru": ("Peru", "PER"),
    "India": ("India", "IND"),
    "New Caledonia": ("New Caledonia", "NCL"),
    "Zimbabwe": ("Zimbabwe", "ZWE"),
    "Mozambique": ("Mozambique", "MOZ"),
    "Madagascar": ("Madagascar", "MDG"),
    "Turkey": ("Türkiye", "TUR"),
    "Türkiye": ("Türkiye", "TUR"),
    "Thailand": ("Thailand", "THA"),
    "Bolivia": ("Bolivia", "BOL"),
    "Argentina": ("Argentina", "ARG"),
    "Vietnam": ("Vietnam", "VNM"),
    "Viet Nam": ("Vietnam", "VNM"),
    "Cuba": ("Cuba", "CUB"),
    "Zambia": ("Zambia", "ZMB"),
    "Korea (Rep. of)": ("South Korea", "KOR"),
    "Republic of Korea": ("South Korea", "KOR"),
    "Korea, Republic of": ("South Korea", "KOR"),
    "South Korea": ("South Korea", "KOR"),
    "Korea, Dem. P.R. of": ("North Korea", "PRK"),
    "Dem. People's Rep. of Korea": ("North Korea", "PRK"),
    "Finland": ("Finland", "FIN"),
    "Ukraine": ("Ukraine", "UKR"),
    "Israel": ("Israel", "ISR"),
    "Gabon": ("Gabon", "GAB"),
    "Mexico": ("Mexico", "MEX"),
    "Rwanda": ("Rwanda", "RWA"),
    "Spain": ("Spain", "ESP"),
    "Portugal": ("Portugal", "PRT"),
    "Colombia": ("Colombia", "COL"),
    "Papua New Guinea": ("PNG", "PNG"),
    "Belgium": ("Belgium", "BEL"),
    "Norway": ("Norway", "NOR"),
    "Germany": ("Germany", "DEU"),
    "France": ("France", "FRA"),
    "United Kingdom": ("UK", "GBR"),
    "Italy": ("Italy", "ITA"),
    "Netherlands": ("Netherlands", "NLD"),
    "Morocco": ("Morocco", "MAR"),
    "Kazakhstan": ("Kazakhstan", "KAZ"),
    "Mongolia": ("Mongolia", "MNG"),
    "Laos": ("Laos", "LAO"),
    "Lao People's Dem. Rep.": ("Laos", "LAO"),
    "Greenland": ("Greenland", "GRL"),
    "Sweden": ("Sweden", "SWE"),
    "Austria": ("Austria", "AUT"),
    "Czech Republic": ("Czechia", "CZE"),
    "Czechia": ("Czechia", "CZE"),
    "Poland": ("Poland", "POL"),
    "Slovakia": ("Slovakia", "SVK"),
    "Bulgaria": ("Bulgaria", "BGR"),
    "Iran": ("Iran", "IRN"),
    "Iran, Islamic Rep. of": ("Iran", "IRN"),
    "Saudi Arabia": ("Saudi Arabia", "SAU"),
    "Egypt": ("Egypt", "EGY"),
    "Tanzania": ("Tanzania", "TZA"),
    "United Rep. of Tanzania": ("Tanzania", "TZA"),
    "Burundi": ("Burundi", "BDI"),
    "Uganda": ("Uganda", "UGA"),
    "Ethiopia": ("Ethiopia", "ETH"),
    "Nigeria": ("Nigeria", "NGA"),
    "Ghana": ("Ghana", "GHA"),
    "Sudan": ("Sudan", "SDN"),
    "Botswana": ("Botswana", "BWA"),
    "Namibia": ("Namibia", "NAM"),
    "Malaysia": ("Malaysia", "MYS"),
    "Singapore": ("Singapore", "SGP"),
    "Hong Kong": ("Hong Kong", "HKG"),
    "China, Hong Kong SAR": ("Hong Kong", "HKG"),
    "Taiwan": ("Taiwan", "TWN"),
    "Other Asia, nes": ("Taiwan", "TWN"),
    "Cambodia": ("Cambodia", "KHM"),
    "Pakistan": ("Pakistan", "PAK"),
    "Sri Lanka": ("Sri Lanka", "LKA"),
    "Bangladesh": ("Bangladesh", "BGD"),
    "Greece": ("Greece", "GRC"),
    "Romania": ("Romania", "ROU"),
    "Hungary": ("Hungary", "HUN"),
    "Switzerland": ("Switzerland", "CHE"),
    "Denmark": ("Denmark", "DNK"),
    "Ireland": ("Ireland", "IRL"),
    "Estonia": ("Estonia", "EST"),
    "Lithuania": ("Lithuania", "LTU"),
    "Latvia": ("Latvia", "LVA"),
    "Slovenia": ("Slovenia", "SVN"),
    "Croatia": ("Croatia", "HRV"),
    "Serbia": ("Serbia", "SRB"),
    "Bosnia Herzegovina": ("Bosnia & Herzegovina", "BIH"),
    "Albania": ("Albania", "ALB"),
    "North Macedonia": ("North Macedonia", "MKD"),
    "Belarus": ("Belarus", "BLR"),
    "Georgia": ("Georgia", "GEO"),
    "Armenia": ("Armenia", "ARM"),
    "Azerbaijan": ("Azerbaijan", "AZE"),
    "Uzbekistan": ("Uzbekistan", "UZB"),
    "Kyrgyzstan": ("Kyrgyzstan", "KGZ"),
    "Turkmenistan": ("Turkmenistan", "TKM"),
    "Afghanistan": ("Afghanistan", "AFG"),
    "Venezuela": ("Venezuela", "VEN"),
    "Ecuador": ("Ecuador", "ECU"),
    "Guatemala": ("Guatemala", "GTM"),
    "Honduras": ("Honduras", "HND"),
    "Dominican Republic": ("Dominican Republic", "DOM"),
    "Jamaica": ("Jamaica", "JAM"),
    "Trinidad and Tobago": ("Trinidad & Tobago", "TTO"),
    "Suriname": ("Suriname", "SUR"),
    "Guyana": ("Guyana", "GUY"),
    "Algeria": ("Algeria", "DZA"),
    "Tunisia": ("Tunisia", "TUN"),
    "Libya": ("Libya", "LBY"),
    "Senegal": ("Senegal", "SEN"),
    "Mali": ("Mali", "MLI"),
    "Burkina Faso": ("Burkina Faso", "BFA"),
    "Niger": ("Niger", "NER"),
    "Côte d'Ivoire": ("Côte d'Ivoire", "CIV"),
    "Cote d'Ivoire": ("Côte d'Ivoire", "CIV"),
    "Liberia": ("Liberia", "LBR"),
    "Sierra Leone": ("Sierra Leone", "SLE"),
    "Guinea": ("Guinea", "GIN"),
    "Mauritania": ("Mauritania", "MRT"),
    "Cameroon": ("Cameroon", "CMR"),
    "Central African Republic": ("CAR", "CAF"),
    "Chad": ("Chad", "TCD"),
    "Eritrea": ("Eritrea", "ERI"),
    "Somalia": ("Somalia", "SOM"),
    "Kenya": ("Kenya", "KEN"),
    "Malawi": ("Malawi", "MWI"),
    "Lesotho": ("Lesotho", "LSO"),
    "Eswatini": ("Eswatini", "SWZ"),
    "Angola": ("Angola", "AGO"),
    "Yemen": ("Yemen", "YEM"),
    "Oman": ("Oman", "OMN"),
    "United Arab Emirates": ("UAE", "ARE"),
    "Qatar": ("Qatar", "QAT"),
    "Kuwait": ("Kuwait", "KWT"),
    "Bahrain": ("Bahrain", "BHR"),
    "Iraq": ("Iraq", "IRQ"),
    "Syria": ("Syria", "SYR"),
    "Jordan": ("Jordan", "JOR"),
    "Lebanon": ("Lebanon", "LBN"),
    "Cyprus": ("Cyprus", "CYP"),
    "Iceland": ("Iceland", "ISL"),
    "New Zealand": ("New Zealand", "NZL"),
    "Fiji": ("Fiji", "FJI"),
    "Solomon Islands": ("Solomon Islands", "SLB"),
    "Yugoslavia (former)": ("Yugoslavia (former)", "YUG"),
    "Czechoslovakia (former)": ("Czechoslovakia (former)", "CSK"),
}


def harmonize_country(name):
    """Return (canonical_name, iso3) or (name, None) if not in map."""
    if pd.isna(name):
        return (None, None)
    name = str(name).strip()
    if name in COUNTRY_HARMONIZATION:
        return COUNTRY_HARMONIZATION[name]
    return (name, None)


def classify_stage(commodity_label):
    """Classify a BGS bgs_commodity_trans value into 'mining' or 'refining'.
    Returns None if no keyword matches.

    IMPORTANT: mining keywords are checked first because labels like
    'platinum group metals, mine' contain 'metal' but are clearly mining.
    """
    if pd.isna(commodity_label):
        return None
    label = str(commodity_label).lower()
    # Mining first — 'mine', 'ore', 'concentrate' are the strongest signals
    for kw in STAGE_PATTERNS["mining"]:
        if kw in label:
            return "mining"
    for kw in STAGE_PATTERNS["refining"]:
        if kw in label:
            return "refining"
    return None


# ---------------------------------------------------------------------
# Processing functions
# ---------------------------------------------------------------------

def load_bgs_for_mineral(mineral_key, file_list):
    """Load and concatenate BGS production files for a mineral.
    Returns DataFrame with columns: country, iso3, year, stage, quantity, units, sub_commodity.
    """
    frames = []
    for fname in file_list:
        path = SOURCE_DIR / fname
        if not path.exists():
            print(f"  [skip] {fname} not found")
            continue
        df = pd.read_csv(path, low_memory=False)
        df["year_int"] = pd.to_datetime(df["year"], errors="coerce").dt.year
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df = df[(df["year_int"] >= YEAR_MIN) & (df["year_int"] <= YEAR_MAX)]
        df = df.dropna(subset=["quantity", "country_trans"])

        # For ferroalloys file, we need to filter on sub_commodity (not commodity).
        # The bgs_commodity_trans is just 'ferro-alloys' for everything; the
        # actual alloy type lives in bgs_sub_commodity_trans. For manganese we
        # keep only the separate ferromanganese and silicomanganese rows;
        # combined-reporting rows (e.g. "Ferro-manganese & ferro-silico-manganese")
        # are dropped to avoid double-counting against countries that report them
        # separately. NOTE: China appears to be absent from BGS ferroalloys data;
        # this is a known data limitation, not a build bug.
        if "ferroalloys" in fname.lower() and mineral_key == "manganese":
            mn_subcomms = [
                "Ferro-manganese",
                "Ferro-silico-manganese",
                "Ferro-Alloys (Silico-manganese)",
            ]
            df = df[df["bgs_sub_commodity_trans"].isin(mn_subcomms)]

        df["stage"] = df["bgs_commodity_trans"].apply(classify_stage)

        # Apply per-mineral override if defined
        override = MINERAL_STAGE_OVERRIDES.get(mineral_key)
        if override is not None:
            kind, target = override
            if kind == "force_all":
                # All rows from this file get forced to a specific stage,
                # regardless of how classify_stage labelled them.
                df["stage"] = target
            elif kind == "drop_stage":
                # Remove rows assigned to the target stage entirely.
                df = df[df["stage"] != target]
            elif kind == "force_stage":
                # Reassign rows that were not classified to the target stage.
                df.loc[df["stage"].isna(), "stage"] = target

        # Harmonize country
        harmonized = df["country_trans"].apply(harmonize_country)
        df["country"] = harmonized.apply(lambda x: x[0])
        df["iso3"] = harmonized.apply(lambda x: x[1])

        # Drop rows whose stage couldn't be classified
        df = df.dropna(subset=["stage", "country"])

        frames.append(df[["country", "iso3", "year_int", "stage", "quantity", "units",
                          "bgs_commodity_trans", "bgs_sub_commodity_trans"]]
                      .rename(columns={"year_int": "year",
                                       "bgs_commodity_trans": "commodity_label",
                                       "bgs_sub_commodity_trans": "sub_commodity"}))

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def aggregate_production(df, stage):
    """Aggregate to country-year totals for a given stage.
    Returns dict: {year: [{country, iso3, quantity, share_pct}, ...]} (top 12 + Other).
    """
    sub = df[df["stage"] == stage].copy()
    if sub.empty:
        return None

    # Sum across sub-commodities within country-year
    agg = sub.groupby(["country", "iso3", "year"])["quantity"].sum().reset_index()

    # Determine canonical units (use most common label across the data)
    units_label = sub["units"].mode().iloc[0] if not sub["units"].mode().empty else "tonnes"

    # Top countries by total over the window (used for stable color/order across years)
    totals = agg.groupby(["country", "iso3"])["quantity"].sum().sort_values(ascending=False)
    top_countries = totals.head(12).index.tolist()  # list of (country, iso3)
    top_country_names = [c[0] for c in top_countries]

    # Build year-by-year picture
    by_year = {}
    for year in sorted(agg["year"].unique()):
        year_df = agg[agg["year"] == year]
        total = year_df["quantity"].sum()
        if total <= 0:
            continue
        rows = []
        other_qty = 0.0
        for _, r in year_df.iterrows():
            entry = {
                "country": r["country"],
                "iso3": r["iso3"],
                "quantity": float(r["quantity"]),
                "share_pct": float(r["quantity"] / total * 100),
            }
            if r["country"] in top_country_names:
                rows.append(entry)
            else:
                other_qty += r["quantity"]
        rows.sort(key=lambda x: -x["quantity"])
        if other_qty > 0:
            rows.append({
                "country": "Other",
                "iso3": None,
                "quantity": float(other_qty),
                "share_pct": float(other_qty / total * 100),
            })
        by_year[int(year)] = {
            "total": float(total),
            "countries": rows,
        }

    # Time series per top country (long form, easier for JS)
    series = {}
    for country in top_country_names:
        country_data = agg[agg["country"] == country].set_index("year")["quantity"]
        series[country] = {int(y): float(country_data.loc[y]) if y in country_data.index else 0.0
                           for y in range(YEAR_MIN, YEAR_MAX + 1)}
    # Other
    other_series = {}
    for year in range(YEAR_MIN, YEAR_MAX + 1):
        year_df = agg[agg["year"] == year]
        other_qty = year_df[~year_df["country"].isin(top_country_names)]["quantity"].sum()
        other_series[year] = float(other_qty) if pd.notna(other_qty) else 0.0
    if any(v > 0 for v in other_series.values()):
        series["Other"] = other_series

    # Concentration metrics by year
    metrics = {}
    for year in range(YEAR_MIN, YEAR_MAX + 1):
        year_df = agg[agg["year"] == year]
        total = year_df["quantity"].sum()
        if total <= 0:
            continue
        shares = (year_df["quantity"] / total * 100).values
        hhi = float(np.sum(shares ** 2))
        sorted_shares = np.sort(shares)[::-1]
        top1 = float(sorted_shares[0]) if len(sorted_shares) > 0 else 0.0
        top3 = float(sorted_shares[:3].sum()) if len(sorted_shares) > 0 else 0.0
        top1_country = year_df.loc[year_df["quantity"].idxmax(), "country"]
        metrics[int(year)] = {
            "hhi": hhi,
            "top1_share": top1,
            "top3_share": top3,
            "top1_country": top1_country,
            "n_producers": int(len(year_df)),
        }

    return {
        "units": units_label,
        "by_year": by_year,
        "series": series,
        "metrics": metrics,
    }


def process_trade(mineral_key, trade_filename):
    """Load UN Comtrade file, return aggregated trade summary."""
    path = SOURCE_DIR / trade_filename
    if not path.exists():
        print(f"  [skip] {trade_filename} not found")
        return None

    df = pd.read_csv(path, low_memory=False)
    df = df[(df["period"] >= YEAR_MIN) & (df["period"] <= YEAR_MAX)]
    df = df[df["partnerISO"] != "W00"]  # exclude "World" totals; keep bilateral only
    df["primaryValue"] = pd.to_numeric(df["primaryValue"], errors="coerce")
    df = df.dropna(subset=["primaryValue", "reporterDesc"])

    # Harmonize country names
    df["reporter_canonical"] = df["reporterDesc"].apply(lambda x: harmonize_country(x)[0])
    df = df.dropna(subset=["reporter_canonical"])

    out = {}
    for stage in ["primary", "refined"]:
        stage_df = df[df["stage"] == stage]
        if stage_df.empty:
            continue

        # Top exporters by year: aggregate across HS codes in the stage
        exports = stage_df[stage_df["flowCode"] == "X"]
        # Top importers by year
        imports = stage_df[stage_df["flowCode"] == "M"]

        def top_n_by_year(sub, n=10):
            result = {}
            for year in range(YEAR_MIN, YEAR_MAX + 1):
                year_df = sub[sub["period"] == year]
                if year_df.empty:
                    continue
                agg = year_df.groupby("reporter_canonical")["primaryValue"].sum().sort_values(ascending=False)
                total = agg.sum()
                if total <= 0:
                    continue
                top = agg.head(n)
                result[year] = {
                    "total_usd": float(total),
                    "top": [{"country": c, "value_usd": float(v), "share_pct": float(v/total*100)}
                            for c, v in top.items()]
                }
            return result

        out[stage] = {
            "exporters": top_n_by_year(exports, n=10),
            "importers": top_n_by_year(imports, n=10),
            "hs_codes_used": sorted(stage_df["hs_code_queried"].dropna().unique().astype(int).tolist()),
            "hs_descriptions": stage_df.drop_duplicates("hs_code_queried")[["hs_code_queried", "hs_description"]]
                                       .set_index("hs_code_queried")["hs_description"].to_dict(),
        }

    return out if out else None


def process_mineral(mineral_key, config):
    """Build the full mineral profile (mining + refining + trade) and return as dict."""
    print(f"Processing {config['label']} ({mineral_key})...")
    profile = {
        "key": mineral_key,
        "label": config["label"],
        "year_min": YEAR_MIN,
        "year_max": YEAR_MAX,
    }

    # Production
    bgs = load_bgs_for_mineral(mineral_key, config["bgs"])
    if bgs.empty:
        print(f"  [warn] no BGS data for {mineral_key}")
        profile["mining"] = None
        profile["refining"] = None
    else:
        profile["mining"] = aggregate_production(bgs, "mining")
        profile["refining"] = aggregate_production(bgs, "refining")
        if profile["mining"] is None:
            print(f"  [warn] no mining rows for {mineral_key}")
        if profile["refining"] is None:
            print(f"  [info] no refining rows for {mineral_key} (expected for some minerals)")

        # Capture sub-commodity breakdown for transparency (lithium, REE)
        if mineral_key in ("lithium", "ree"):
            sub_breakdown = (bgs.groupby(["stage", "sub_commodity"])["quantity"].sum()
                             .reset_index()
                             .sort_values("quantity", ascending=False))
            profile["sub_commodity_breakdown"] = [
                {"stage": r["stage"], "sub_commodity": (r["sub_commodity"] if pd.notna(r["sub_commodity"]) else "(unspecified)"),
                 "total_quantity": float(r["quantity"])}
                for _, r in sub_breakdown.iterrows()
            ]

    # Trade
    profile["trade"] = process_trade(mineral_key, config["trade"])
    if profile["trade"] is None:
        print(f"  [info] no trade data for {mineral_key}")

    return profile


# ---------------------------------------------------------------------
# Cross-mineral overview (Layer 0)
# ---------------------------------------------------------------------

def build_overview(profiles):
    """For each mineral × stage, extract latest-year top1 share and HHI."""
    rows = []
    for key, p in profiles.items():
        for stage in ("mining", "refining"):
            stage_data = p.get(stage)
            if stage_data is None:
                rows.append({
                    "mineral": p["label"],
                    "mineral_key": key,
                    "stage": stage,
                    "available": False,
                })
                continue
            metrics = stage_data["metrics"]
            if not metrics:
                rows.append({
                    "mineral": p["label"],
                    "mineral_key": key,
                    "stage": stage,
                    "available": False,
                })
                continue
            latest_year = max(metrics.keys())
            m = metrics[latest_year]
            rows.append({
                "mineral": p["label"],
                "mineral_key": key,
                "stage": stage,
                "available": True,
                "latest_year": latest_year,
                "top1_country": m["top1_country"],
                "top1_share": m["top1_share"],
                "hhi": m["hhi"],
            })
    return rows


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Source dir: {SOURCE_DIR}")
    print(f"Output dir: {OUTPUT_DIR}")
    print(f"Window: {YEAR_MIN}-{YEAR_MAX}")
    print()

    profiles = {}
    for key, cfg in MINERALS.items():
        profile = process_mineral(key, cfg)
        profiles[key] = profile
        # Write per-mineral file
        with open(OUTPUT_DIR / f"{key}.json", "w") as f:
            json.dump(profile, f, separators=(",", ":"), default=str)

    # Overview
    overview = build_overview(profiles)
    with open(OUTPUT_DIR / "overview.json", "w") as f:
        json.dump({"overview": overview, "year_min": YEAR_MIN, "year_max": YEAR_MAX},
                  f, indent=2, default=str)

    # Manifest (which minerals have which stages)
    manifest = {
        "year_min": YEAR_MIN,
        "year_max": YEAR_MAX,
        "minerals": [
            {
                "key": key,
                "label": p["label"],
                "has_mining": p.get("mining") is not None,
                "has_refining": p.get("refining") is not None,
                "has_trade": p.get("trade") is not None,
            }
            for key, p in profiles.items()
        ],
        "build_notes": (
            "Generated by build_dashboard_data.py. Window 2000-2023. "
            "BGS = British Geological Survey World Mineral Statistics. "
            "UN Comtrade = bilateral trade flows by HS code. "
            "Production stages classified from bgs_commodity_trans labels: "
            "see code/build_dashboard_data.py for STAGE_PATTERNS."
        ),
    }
    with open(OUTPUT_DIR / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    print()
    print("Done. Files written:")
    for p in sorted(OUTPUT_DIR.glob("*.json")):
        size_kb = p.stat().st_size / 1024
        print(f"  {p.name}  ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
