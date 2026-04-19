"""
CriMinGeo - Reserves vs Production Analysis (USGS MCS 2025)
===========================================================
This script compares where reserves are vs where production happens,
highlighting the gap between geological endowment and active extraction.

Data source: USGS Mineral Commodity Summaries 2025 (World Data CSV)
"""

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

# Resolve paths from script location so execution works from any cwd.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 10,
    "figure.dpi": 150,
    "savefig.dpi": 150,
    "savefig.pad_inches": 0.3,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

OUTPUT = os.path.join(PROJECT_ROOT, "results", "figures", "mcs2025_reserves")
os.makedirs(OUTPUT, exist_ok=True)

MCS_FILE = os.path.join(PROJECT_ROOT, "data", "source", "MCS2025_World_Data.csv")

COLORS = {
    "China": "#DE2910",
    "Congo (Kinshasa)": "#007FFF",
    "Australia": "#FFB81C",
    "Chile": "#D52B1E",
    "Indonesia": "#CC0000",
    "South Africa": "#007749",
    "Russia": "#0039A6",
    "Burma": "#FEC50D",
    "United States": "#3C3B6E",
    "Brazil": "#009C3B",
    "Philippines": "#0038A8",
    "Canada": "#EE0000",
    "Tajikistan": "#006847",
    "India": "#FF9933",
    "Others": "#BBBBBB",
    "Zimbabwe": "#FFD200",
    "Mozambique": "#009639",
    "Madagascar": "#FC3D32",
    "Turkey": "#E30A17",
    "Bolivia": "#007934",
    "Argentina": "#74ACDF",
    "Gabon": "#009E60",
    "Ghana": "#CE1126",
    "Nigeria": "#008751",
    "Thailand": "#2D2926",
    "Tanzania": "#00A3DD",
    "Cuba": "#002590",
    "Kyrgyzstan": "#EF3340",
    "Peru": "#D91023",
    "Kazakhstan": "#00AFCA",
    "Iran": "#239F40",
    "Israel": "#0038B8",
    "New Caledonia": "#009543",
    "World total (rounded)": "#333333",
    "Other Countries": "#BBBBBB",
}

MINERAL_COLORS_MAP = {
    "Rare Earths": "#DE2910",
    "Lithium": "#2196F3",
    "Cobalt": "#4CAF50",
    "Nickel": "#607D8B",
    "Graphite": "#333333",
    "Copper": "#795548",
    "Tungsten": "#FF9800",
    "Antimony": "#9C27B0",
    "PGMs": "#00BCD4",
    "Manganese": "#8BC34A",
    "Magnesium": "#FF5722",
}


def gc(country):
    return COLORS.get(country, "#888888")


def normalize_text(series):
    return (
        series.astype(str)
        .str.replace("\u00a0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def clean_numeric(series):
    cleaned = (
        series.astype(str)
        .str.replace("\u00a0", " ", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    # Extract the first numeric token from entries such as ">90000000" or "1200 (estimated)".
    numeric_token = cleaned.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(numeric_token, errors="coerce")


def unit_multiplier(unit_text):
    u = str(unit_text).lower().strip()
    if "million metric tons" in u:
        return 1_000_000.0
    if "thousand metric tons" in u:
        return 1_000.0
    if "metric tons" in u:
        return 1.0
    if "million kilograms" in u:
        return 1_000.0
    if "thousand kilograms" in u:
        return 1.0
    if "kilograms" in u:
        return 0.001
    return 1.0


# ==========================================================
# LOAD AND PROCESS MCS 2025
# ==========================================================
print("Loading MCS 2025 data...")

df = pd.read_csv(MCS_FILE)

df["COUNTRY"] = normalize_text(df["COUNTRY"])
df["COMMODITY"] = normalize_text(df["COMMODITY"])
df["TYPE"] = normalize_text(df["TYPE"])
df["UNIT_MEAS"] = normalize_text(df["UNIT_MEAS"])

# Define minerals and corresponding USGS names + TYPE filter.
mineral_config = {
    "Rare Earths": {
        "usgs_names": ["Rare earths"],
        "type_keywords": ["mine production", "oxide equivalent"],
    },
    "Lithium": {
        "usgs_names": ["Lithium"],
        "type_keywords": ["mine production", "lithium content"],
    },
    "Cobalt": {
        "usgs_names": ["Cobalt"],
        "type_keywords": ["mine production"],
    },
    "Nickel": {
        "usgs_names": ["Nickel"],
        "type_keywords": ["mine production"],
    },
    "Graphite": {
        "usgs_names": ["Graphite"],
        "type_keywords": ["mine production"],
    },
    "Copper": {
        "usgs_names": ["Copper"],
        "type_keywords": ["mine production", "recoverable", "copper content"],
    },
    "Gallium": {
        "usgs_names": ["Gallium"],
        "type_keywords": ["primary production", "mine production", "production"],
    },
    "Germanium": {
        "usgs_names": ["Germanium"],
        "type_keywords": ["production"],
    },
    "Tungsten": {
        "usgs_names": ["Tungsten"],
        "type_keywords": ["mine production", "tungsten content"],
    },
    "Antimony": {
        "usgs_names": ["Antimony"],
        "type_keywords": ["mine production"],
    },
    "PGMs": {
        "usgs_names": ["Platinum-Group metals"],
        "type_keywords": ["mine production"],
    },
    "Manganese": {
        "usgs_names": ["Manganese"],
        "type_keywords": ["mine production"],
    },
    "Magnesium": {
        "usgs_names": ["Magnesium metal"],
        "type_keywords": ["smelter production", "production"],
    },
}

PROD_COL = "PROD_EST_ 2024"
RES_COL = "RESERVES_2024"
UNIT_COL = "UNIT_MEAS"



def filter_mcs_rows(config):
    subset = df[df["COMMODITY"].isin(config["usgs_names"])].copy()

    keywords = config.get("type_keywords", [])
    if keywords and len(subset) > 0:
        mask = pd.Series(False, index=subset.index)
        for kw in keywords:
            mask = mask | subset["TYPE"].str.contains(kw, case=False, na=False)
        filtered = subset[mask]
        if len(filtered) > 0:
            subset = filtered

    return subset


def get_mineral_data(config):
    """Extract country-level production and reserves for one mineral."""
    subset = filter_mcs_rows(config)

    subset["res_clean"] = clean_numeric(subset[RES_COL])
    subset["prod_clean"] = clean_numeric(subset[PROD_COL])

    # Remove world and aggregate rows for country-level analysis.
    countries = subset[
        ~subset["COUNTRY"].str.contains("World|total|Other Countries", case=False, na=False)
    ].copy()

    return countries


# ==========================================================
# Process all minerals
# ==========================================================
print("Processing minerals...")

all_mineral_data = {}
for our_name, config in mineral_config.items():
    data = get_mineral_data(config)
    if len(data) > 0:
        all_mineral_data[our_name] = data
        prod_countries = data["prod_clean"].notna().sum()
        res_countries = data["res_clean"].notna().sum()
        print(f"  {our_name:15s}: {prod_countries} countries with production, {res_countries} with reserves")

minerals_with_both = [
    m for m in all_mineral_data
    if all_mineral_data[m]["res_clean"].notna().any() and all_mineral_data[m]["prod_clean"].notna().any()
]

if not minerals_with_both:
    raise RuntimeError("No minerals found with both reserves and production in MCS 2025 data.")


# ==========================================================
# FIGURE R1: Reserves vs Production, one large figure per mineral
# ==========================================================
print("\nGenerating Figure R1: one figure per mineral (reserves top, production bottom)...")

for mineral in minerals_with_both:
    data = all_mineral_data[mineral]
    fig, (ax_res, ax_prod) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    # TOP: Reserves
    res_data = data[data["res_clean"].notna()].sort_values("res_clean", ascending=False).head(10)
    total_res = res_data["res_clean"].sum()

    if total_res > 0:
        countries_r = res_data["COUNTRY"].tolist()
        shares_r = (res_data["res_clean"] / total_res * 100).tolist()
        colors_r = [gc(c) for c in countries_r]
        y_r = np.arange(len(countries_r))

        bars = ax_res.barh(y_r, shares_r, color=colors_r, edgecolor="white", linewidth=0.5)
        ax_res.set_yticks(y_r)
        ax_res.set_yticklabels(countries_r, fontsize=9)
        ax_res.invert_yaxis()

        for bar, share in zip(bars, shares_r):
            if share > 3:
                ax_res.text(
                    bar.get_width() + 0.8,
                    bar.get_y() + bar.get_height() / 2,
                    f"{share:.1f}%",
                    ha="left",
                    va="center",
                    fontsize=8,
                    fontweight="bold",
                )

    ax_res.set_title(f"{mineral} - RESERVES", fontsize=12, fontweight="bold")
    ax_res.set_xlabel("Share of known reserves (%)", fontsize=10)
    ax_res.grid(axis="x", alpha=0.2, linestyle="--")

    # BOTTOM: Production
    prod_data = data[data["prod_clean"].notna()].sort_values("prod_clean", ascending=False).head(10)
    total_prod = prod_data["prod_clean"].sum()

    if total_prod > 0:
        countries_p = prod_data["COUNTRY"].tolist()
        shares_p = (prod_data["prod_clean"] / total_prod * 100).tolist()
        colors_p = [gc(c) for c in countries_p]
        y_p = np.arange(len(countries_p))

        bars = ax_prod.barh(y_p, shares_p, color=colors_p, edgecolor="white", linewidth=0.5)
        ax_prod.set_yticks(y_p)
        ax_prod.set_yticklabels(countries_p, fontsize=9)
        ax_prod.invert_yaxis()

        for bar, share in zip(bars, shares_p):
            if share > 3:
                ax_prod.text(
                    bar.get_width() + 0.8,
                    bar.get_y() + bar.get_height() / 2,
                    f"{share:.1f}%",
                    ha="left",
                    va="center",
                    fontsize=8,
                    fontweight="bold",
                )

    ax_prod.set_title(f"{mineral} - PRODUCTION (2024e)", fontsize=12, fontweight="bold")
    ax_prod.set_xlabel("Share of global production (%)", fontsize=10)
    ax_prod.grid(axis="x", alpha=0.2, linestyle="--")

    fig.suptitle(
        f"{mineral}: Where Reserves Are vs Where Production Happens (USGS MCS 2025)",
        fontsize=13,
        fontweight="bold",
        y=0.995,
    )
    plt.tight_layout()

    mineral_name = mineral.lower().replace(" ", "_")
    plt.savefig(os.path.join(OUTPUT, f"fig_reserves_vs_production_{mineral_name}.png"), bbox_inches="tight")
    plt.close()

print(f"  ok Figure R1 saved as {len(minerals_with_both)} separate mineral figures")


# ==========================================================
# FIGURE R2: Reserves-to-Production ratio
# ==========================================================
print("Generating Figure R2: Reserves-to-production ratio...")

fig, ax = plt.subplots(figsize=(13, 7))

rp_data = []
for mineral in minerals_with_both:
    data = all_mineral_data[mineral]

    full = filter_mcs_rows(mineral_config[mineral])

    full["world_prod_clean"] = clean_numeric(full[PROD_COL])
    full["world_res_clean"] = clean_numeric(full[RES_COL])

    world_row = full[full["COUNTRY"].str.contains("World|total", case=False, na=False)]

    if len(world_row) > 0:
        world_prod = world_row["world_prod_clean"].iloc[0]
        world_res = world_row["world_res_clean"].iloc[0]
        world_unit = world_row[UNIT_COL].iloc[0] if UNIT_COL in world_row.columns else ""

        if pd.notna(world_prod) and pd.notna(world_res) and world_prod > 0:
            # Harmonize to metric tons before reserves/production ratio.
            unit_mult = unit_multiplier(world_unit)
            rp_ratio = (world_res * unit_mult) / (world_prod * unit_mult)
            rp_data.append(
                {
                    "mineral": mineral,
                    "reserves": world_res,
                    "production": world_prod,
                    "rp_ratio": rp_ratio,
                    "unit": data[UNIT_COL].iloc[0] if UNIT_COL in data.columns else "",
                }
            )

if rp_data:
    rp_df = pd.DataFrame(rp_data).sort_values("rp_ratio", ascending=True)

    y_pos = np.arange(len(rp_df))
    colors = [MINERAL_COLORS_MAP.get(m, "#666") for m in rp_df["mineral"]]

    bars = ax.barh(y_pos, rp_df["rp_ratio"], color=colors, edgecolor="white", alpha=0.85)

    for bar, ratio in zip(bars, rp_df["rp_ratio"]):
        ax.text(
            bar.get_width() + 2,
            bar.get_y() + bar.get_height() / 2,
            f"{ratio:.0f} years",
            va="center",
            fontsize=10,
            fontweight="bold",
        )

    ax.set_yticks(y_pos)
    ax.set_yticklabels(rp_df["mineral"], fontsize=11)
    ax.set_xlabel("Years of supply at current production rates", fontsize=11)

    ax.axvline(x=25, color="orange", linestyle="--", alpha=0.5)
    ax.text(26, len(rp_df) - 0.5, "25 years\n(1 generation)", fontsize=8, color="orange")
    ax.axvline(x=50, color="red", linestyle="--", alpha=0.4)
    ax.text(51, len(rp_df) - 0.5, "50 years", fontsize=8, color="red")

    ax.set_title(
        "How Long Will Reserves Last at Current Production? (USGS MCS 2025)\n"
        "Reserves / Annual production; demand growth may reduce effective years",
        fontsize=13,
        fontweight="bold",
        pad=12,
    )

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT, "fig_reserves_production_ratio.png"), bbox_inches="tight")
plt.close()
print("  ok Figure R2 saved")


# ==========================================================
# FIGURE R3: Reserves-production mismatch
# ==========================================================
print("Generating Figure R3: Reserves-production mismatch...")

fig, ax = plt.subplots(figsize=(14, 8))

mismatch_data = []
for mineral in minerals_with_both:
    data = all_mineral_data[mineral]

    res_total = data["res_clean"].sum()
    prod_total = data["prod_clean"].sum()

    if res_total > 0 and prod_total > 0:
        for _, row in data.iterrows():
            country = row["COUNTRY"]
            res_share = (row["res_clean"] / res_total * 100) if pd.notna(row["res_clean"]) else 0
            prod_share = (row["prod_clean"] / prod_total * 100) if pd.notna(row["prod_clean"]) else 0

            if res_share > 5 or prod_share > 5:
                mismatch_data.append(
                    {
                        "mineral": mineral,
                        "country": country,
                        "reserve_share": res_share,
                        "production_share": prod_share,
                        "gap": prod_share - res_share,
                    }
                )

mismatch_df = pd.DataFrame(mismatch_data)

if not mismatch_df.empty:
    for mineral in minerals_with_both:
        subset = mismatch_df[mismatch_df["mineral"] == mineral]
        if subset.empty:
            continue

        color = MINERAL_COLORS_MAP.get(mineral, "#666")
        ax.scatter(
            subset["reserve_share"],
            subset["production_share"],
            s=80,
            color=color,
            alpha=0.7,
            edgecolors="white",
            linewidth=0.5,
            label=mineral,
        )

        for _, row in subset.iterrows():
            if abs(row["gap"]) > 20 or row["production_share"] > 40 or row["reserve_share"] > 40:
                ax.annotate(
                    f"{row['country'][:12]}",
                    (row["reserve_share"], row["production_share"]),
                    fontsize=6.5,
                    alpha=0.8,
                    xytext=(5, 5),
                    textcoords="offset points",
                )

ax.plot([0, 80], [0, 80], "k--", alpha=0.3, linewidth=1)
ax.text(
    60,
    55,
    "Balanced\n(reserves ~= production)",
    fontsize=8,
    color="gray",
    rotation=35,
    alpha=0.5,
)

ax.text(
    5,
    65,
    "<- Overproducing\n   relative to reserves",
    fontsize=9,
    color="#CC0000",
    fontstyle="italic",
    alpha=0.7,
)
ax.text(
    50,
    5,
    "  Underproducing ->\n  relative to reserves",
    fontsize=9,
    color="#006600",
    fontstyle="italic",
    alpha=0.7,
)

ax.set_xlabel("Share of global reserves (%)", fontsize=12)
ax.set_ylabel("Share of global production (%)", fontsize=12)
ax.set_xlim(-2, 80)
ax.set_ylim(-2, 82)
ax.legend(loc="upper left", fontsize=8, framealpha=0.9, ncol=2)
ax.set_title(
    "The Reserves-Production Mismatch\n"
    "Points above diagonal: producing more than reserve share; below: untapped potential",
    fontsize=12,
    fontweight="bold",
    pad=12,
)

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT, "fig_reserves_production_mismatch.png"), bbox_inches="tight")
plt.close()
print("  ok Figure R3 saved")


# ==========================================================
# FIGURE R4: China reserves vs production share
# ==========================================================
print("Generating Figure R4: China reserves vs production position...")

fig, ax = plt.subplots(figsize=(13, 7))

china_data = []
for mineral in minerals_with_both:
    data = all_mineral_data[mineral]
    china = data[data["COUNTRY"] == "China"]

    res_total = data["res_clean"].sum()
    prod_total = data["prod_clean"].sum()

    china_res_share = 0
    china_prod_share = 0

    if len(china) > 0:
        if res_total > 0 and pd.notna(china["res_clean"].iloc[0]):
            china_res_share = china["res_clean"].iloc[0] / res_total * 100
        if prod_total > 0 and pd.notna(china["prod_clean"].iloc[0]):
            china_prod_share = china["prod_clean"].iloc[0] / prod_total * 100

    china_data.append(
        {
            "mineral": mineral,
            "reserve_share": china_res_share,
            "production_share": china_prod_share,
            "gap": china_prod_share - china_res_share,
        }
    )

china_df = pd.DataFrame(china_data).sort_values("production_share", ascending=True)
y_pos = np.arange(len(china_df))

for i, (_, row) in enumerate(china_df.iterrows()):
    ax.plot([row["reserve_share"], row["production_share"]], [i, i], color="#333", linewidth=2, alpha=0.4)
    ax.scatter(row["reserve_share"], i, s=120, color="#FFB81C", edgecolors="#333", linewidth=0.8, zorder=3)
    ax.scatter(row["production_share"], i, s=120, color="#DE2910", edgecolors="#333", linewidth=0.8, zorder=3)

    gap = row["production_share"] - row["reserve_share"]
    if abs(gap) > 3:
        sign = "+" if gap > 0 else ""
        ax.text(
            max(row["reserve_share"], row["production_share"]) + 2,
            i,
            f"{sign}{gap:.0f}pp",
            fontsize=8,
            va="center",
            color="#DE2910" if gap > 0 else "#006600",
            fontweight="bold",
        )

ax.set_yticks(y_pos)
ax.set_yticklabels(china_df["mineral"], fontsize=10)
ax.set_xlabel("China share (%)", fontsize=11)
ax.set_xlim(-5, 105)
ax.axvline(x=50, color="gray", linestyle=":", alpha=0.3)

legend_elements = [
    Line2D([0], [0], marker="o", color="w", markerfacecolor="#FFB81C", markeredgecolor="#333", markersize=10, label="Share of global reserves"),
    Line2D([0], [0], marker="o", color="w", markerfacecolor="#DE2910", markeredgecolor="#333", markersize=10, label="Share of global production"),
    Line2D([0], [0], color="#333", alpha=0.4, linewidth=2, label="Gap"),
]
ax.legend(handles=legend_elements, loc="lower right", fontsize=10)

ax.set_title(
    "China: Reserves vs Production Share\n"
    "When production (red) exceeds reserves (gold), extraction outpaces reserve share",
    fontsize=12,
    fontweight="bold",
    pad=12,
)

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT, "fig_china_reserves_vs_production.png"), bbox_inches="tight")
plt.close()
print("  ok Figure R4 saved")


# ==========================================================
# Summary table
# ==========================================================
summary_rows = []
for mineral in minerals_with_both:
    data = all_mineral_data[mineral]
    prod_total = data["prod_clean"].sum()
    res_total = data["res_clean"].sum()

    top_prod = data[data["prod_clean"].notna()].sort_values("prod_clean", ascending=False).head(1)
    top_res = data[data["res_clean"].notna()].sort_values("res_clean", ascending=False).head(1)

    summary_rows.append(
        {
            "Mineral": mineral,
            "Top producer": top_prod["COUNTRY"].iloc[0] if not top_prod.empty else "",
            "Top producer share (%)": round((top_prod["prod_clean"].iloc[0] / prod_total * 100), 1) if (not top_prod.empty and prod_total > 0) else np.nan,
            "Top reserve holder": top_res["COUNTRY"].iloc[0] if not top_res.empty else "",
            "Top reserve share (%)": round((top_res["res_clean"].iloc[0] / res_total * 100), 1) if (not top_res.empty and res_total > 0) else np.nan,
            "Countries with production": int(data["prod_clean"].notna().sum()),
            "Countries with reserves": int(data["res_clean"].notna().sum()),
        }
    )

summary_df = pd.DataFrame(summary_rows)
summary_df.to_csv(os.path.join(PROJECT_ROOT, "results", "tables", "mcs2025_reserves_summary.csv"), index=False)
print("Summary table saved to results/tables/mcs2025_reserves_summary.csv")

print("\n" + "=" * 60)
print(f"All reserves figures saved to: {OUTPUT}")
print("=" * 60)
