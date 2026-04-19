"""
CriMinGeo - Harmonize BGS and USGS Mine Production Series (1971-2024)
======================================================================
Builds a single long-format mine production dataset by combining:
- BGS World Mineral Statistics (historical series through 2023)
- USGS MCS 2025 world data (2024 estimated production)

Output files:
- data/processed/mine_production_harmonized_1971_2024.csv
- data/processed/harmonization_summary.csv
"""

import os
import re
from typing import Dict, List, Tuple

import pandas as pd


# Resolve paths from script location so execution works from any cwd.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# -------------------------------------------------------------------
# File and dictionary configuration
# -------------------------------------------------------------------
MINERALS = [
    "Antimony",
    "Cobalt",
    "Copper",
    "Gallium",
    "Germanium",
    "Graphite",
    "Lithium",
    "Magnesium",
    "Manganese",
    "Nickel",
    "PGMs",
    "Rare Earths",
    "Tungsten",
]

# BGS filenames in this repository (source fallback).
BGS_SOURCE_FILE_MAP = {
    "Antimony": "BGS_antimony_production.csv",
    "Cobalt": "BGS_cobalt_production.csv",
    "Copper": "BGS_copper_production.csv",
    "Gallium": "BGS_gallium_production.csv",
    "Germanium": "BGS_germanium_production.csv",
    "Graphite": "BGS_graphite_production.csv",
    "Lithium": "BGS_lithium_production.csv",
    "Magnesium": "BGS_magnesium_production.csv",
    "Manganese": "BGS_manganese_production.csv",
    "Nickel": "BGS_nickel_production.csv",
    "PGMs": "BGS_platinum_production.csv",
    "Rare Earths": "BGS_REs_production.csv",
    "Tungsten": "BGS_tungsten_production.csv",
}

# USGS commodity names requested by the project specification.
MINERAL_TO_USGS_COMMODITY = {
    "Rare Earths": "Rare earths",
    "Cobalt": "Cobalt",
    "Nickel": "Nickel",
    "Copper": "Copper",
    "Graphite": "Graphite",
    "Antimony": "Antimony",
    "Tungsten": "Tungsten",
    "Gallium": "Gallium",
    "Germanium": "Gemanium",  # USGS file uses this typo.
    "Manganese": "Manganese",
    "Magnesium": "Magnesium metal",
    "PGMs": "Platinum-Group metals",
    "Lithium": "Lithium",
}

# Standardize USGS country names to BGS naming.
USGS_TO_BGS_COUNTRY = {
    "Congo (Kinshasa)": "Congo, Democratic Republic",
    "Burma": "Myanmar",
    "United States": "USA",
    "Korea, Republic of": "Korea (Rep. of)",
    "Korea, North": "Korea, Dem. P.R. of",
    "Russian Federation": "Russia",
}

# BGS variant filters requested in the specification.
BGS_COMMODITY_KEEP = {
    "Copper": ["copper, mine"],
    "Cobalt": ["cobalt, mine"],
    "Nickel": ["nickel, mine"],
    "Rare Earths": ["rare earth oxides"],
}

# Minerals where units are not comparable between BGS and USGS.
INCOMPATIBLE_UNIT_MINERALS = {"Lithium", "Manganese"}


# -------------------------------------------------------------------
# Utility functions
# -------------------------------------------------------------------
def normalize_text(series: pd.Series) -> pd.Series:
    """Normalize text (handles non-breaking spaces and repeated whitespace)."""
    return (
        series.astype(str)
        .str.replace("\u00a0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def clean_numeric(series: pd.Series) -> pd.Series:
    """Extract numeric value from strings such as '>9000' or '1200 (est.)'."""
    cleaned = (
        series.astype(str)
        .str.replace("\u00a0", " ", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    token = cleaned.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(token, errors="coerce")


def resolve_bgs_path(mineral: str) -> str:
    """Resolve BGS file path, preferring data/raw/BGS and falling back to data/source."""
    source_name = BGS_SOURCE_FILE_MAP[mineral]

    candidates = [
        os.path.join(PROJECT_ROOT, "data", "raw", "BGS", source_name),
        os.path.join(PROJECT_ROOT, "data", "raw", source_name),
        os.path.join(PROJECT_ROOT, "data", "source", source_name),
    ]

    for path in candidates:
        if os.path.exists(path):
            return path

    raise FileNotFoundError(f"BGS file not found for {mineral}. Tried: {candidates}")


def resolve_mcs_path() -> str:
    """Resolve USGS MCS file path, preferring data/raw and falling back to data/source."""
    candidates = [
        os.path.join(PROJECT_ROOT, "data", "raw", "MCS2025_World_Data.csv"),
        os.path.join(PROJECT_ROOT, "data", "source", "MCS2025_World_Data.csv"),
    ]

    for path in candidates:
        if os.path.exists(path):
            return path

    raise FileNotFoundError(f"MCS 2025 file not found. Tried: {candidates}")


def year_range_text(year_series: pd.Series) -> str:
    """Return year range as a compact string, e.g. '1971-2023'."""
    valid_years = year_series.dropna().astype(int)
    if len(valid_years) == 0:
        return "NA"
    return f"{valid_years.min()}-{valid_years.max()}"


def has_aggregate_country(country: pd.Series) -> pd.Series:
    """Identify aggregate rows in USGS country field."""
    return country.str.contains(r"World|total|Other Countries", case=False, na=False)


# -------------------------------------------------------------------
# BGS loading and cleaning
# -------------------------------------------------------------------
def load_bgs_for_mineral(mineral: str) -> pd.DataFrame:
    """Load one mineral from BGS and return harmonized long-format rows."""
    path = resolve_bgs_path(mineral)
    df = pd.read_csv(path)

    # Normalize key text columns for robust matching.
    for col in ["country_trans", "bgs_commodity_trans", "units", "year"]:
        if col in df.columns:
            df[col] = normalize_text(df[col])

    # Parse year as datetime then extract year.
    df["year"] = pd.to_datetime(df["year"], errors="coerce").dt.year
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    # Keep mine/primary variants only for selected minerals.
    keep_variants = BGS_COMMODITY_KEEP.get(mineral)
    if keep_variants is not None and "bgs_commodity_trans" in df.columns:
        df = df[df["bgs_commodity_trans"].isin(keep_variants)].copy()

    out = df[["country_trans", "year", "quantity", "units"]].copy()
    out = out.dropna(subset=["country_trans", "year", "quantity"]) 

    out = out.rename(columns={"country_trans": "country", "units": "unit"})
    out["mineral"] = mineral
    out["source"] = "BGS"

    return out[["mineral", "country", "year", "quantity", "unit", "source"]]


# -------------------------------------------------------------------
# USGS loading and extraction
# -------------------------------------------------------------------
def load_usgs_mcs() -> pd.DataFrame:
    """Load and normalize MCS 2025 world data."""
    path = resolve_mcs_path()
    df = pd.read_csv(path)

    text_cols = ["SOURCE", "COMMODITY", "COUNTRY", "TYPE", "UNIT_MEAS"]
    for col in text_cols:
        if col in df.columns:
            df[col] = normalize_text(df[col])

    return df


def get_usgs_for_mineral(usgs_df: pd.DataFrame, mineral: str) -> pd.DataFrame:
    """Extract 2024 estimated mine production rows for one mineral from USGS."""
    commodity_name = MINERAL_TO_USGS_COMMODITY[mineral]
    subset = usgs_df[usgs_df["COMMODITY"] == commodity_name].copy()

    # Keep only relevant production types requested by the specification.
    if mineral == "Magnesium":
        type_mask = subset["TYPE"].str.contains("smelter production", case=False, na=False)
    else:
        type_mask = subset["TYPE"].str.contains("Mine production|Primary production", case=False, na=False)

    subset = subset[type_mask].copy()

    # Remove aggregate regions.
    subset = subset[~has_aggregate_country(subset["COUNTRY"])].copy()

    # Handle the exact column name with a space.
    prod_col = "PROD_EST_ 2024"
    if prod_col not in subset.columns:
        raise KeyError("Expected column 'PROD_EST_ 2024' not found in MCS dataset.")

    subset["quantity"] = clean_numeric(subset[prod_col])
    subset = subset.dropna(subset=["COUNTRY", "quantity"]).copy()

    # PGMs: sum platinum + palladium content per country.
    if mineral == "PGMs":
        subset = (
            subset.groupby(["COUNTRY", "UNIT_MEAS"], as_index=False)["quantity"]
            .sum()
            .copy()
        )

    subset = subset.rename(columns={"COUNTRY": "country", "UNIT_MEAS": "unit"})
    subset["country"] = subset["country"].replace(USGS_TO_BGS_COUNTRY)

    subset["mineral"] = mineral
    subset["year"] = 2024
    subset["source"] = "USGS_MCS2025_estimate"

    return subset[["mineral", "country", "year", "quantity", "unit", "source"]]


# -------------------------------------------------------------------
# Main harmonization workflow
# -------------------------------------------------------------------
def main() -> None:
    print("Loading BGS and USGS data for harmonization...")

    usgs_df = load_usgs_mcs()

    harmonized_frames: List[pd.DataFrame] = []
    summary_rows: List[Dict[str, object]] = []
    unmatched_by_mineral: Dict[str, List[str]] = {}
    warnings_list: List[str] = []

    total_usgs_rows_added = 0
    minerals_successfully_harmonized = 0

    for mineral in MINERALS:
        print(f"\nProcessing {mineral}...")

        bgs_long = load_bgs_for_mineral(mineral)
        bgs_countries = set(bgs_long["country"].dropna().unique())

        # Start output for this mineral with BGS rows.
        mineral_out = bgs_long.copy()

        # Extract USGS 2024 rows for diagnostic summary in all cases.
        usgs_2024 = get_usgs_for_mineral(usgs_df, mineral)
        usgs_countries = set(usgs_2024["country"].dropna().unique())

        matched_countries = sorted([c for c in usgs_countries if c in bgs_countries])
        unmatched_countries = sorted([c for c in usgs_countries if c not in bgs_countries])
        unmatched_by_mineral[mineral] = unmatched_countries

        unit_compatible = mineral not in INCOMPATIBLE_UNIT_MINERALS

        if unit_compatible:
            # Append USGS 2024 estimates for comparable minerals.
            mineral_out = pd.concat([mineral_out, usgs_2024], ignore_index=True)
            total_usgs_rows_added += len(usgs_2024)
            minerals_successfully_harmonized += 1
            print(f"  Added {len(usgs_2024)} USGS rows for 2024.")
        else:
            warning_text = (
                f"{mineral}: BGS and USGS units are not directly comparable "
                f"(gross weight vs content). USGS rows were not appended."
            )
            warnings_list.append(warning_text)
            print(f"  WARNING: {warning_text}")

        # Keep required output schema and ensure year is integer.
        mineral_out["year"] = mineral_out["year"].astype(int)
        harmonized_frames.append(mineral_out[["mineral", "country", "year", "quantity", "unit", "source"]])

        summary_rows.append(
            {
                "mineral": mineral,
                "bgs_year_range": year_range_text(bgs_long["year"]),
                "bgs_n_countries": bgs_long["country"].nunique(),
                "usgs_2024_n_countries": usgs_2024["country"].nunique(),
                "n_country_matches": len(matched_countries),
                "n_usgs_unmatched": len(unmatched_countries),
                "unit_compatible_flag": unit_compatible,
            }
        )

    # Merge all minerals into one long-format output.
    harmonized = pd.concat(harmonized_frames, ignore_index=True)
    harmonized = harmonized.sort_values(["mineral", "year", "country"]).reset_index(drop=True)

    output_long = os.path.join(OUTPUT_DIR, "mine_production_harmonized_1971_2024.csv")
    harmonized.to_csv(output_long, index=False)

    summary_df = pd.DataFrame(summary_rows).sort_values("mineral").reset_index(drop=True)
    output_summary = os.path.join(OUTPUT_DIR, "harmonization_summary.csv")
    summary_df.to_csv(output_summary, index=False)

    # ---------------------------------------------------------------
    # Clear terminal summary for research assistants and QA
    # ---------------------------------------------------------------
    print("\n" + "=" * 70)
    print("HARMONIZATION COMPLETE")
    print("=" * 70)
    print(f"Minerals successfully harmonized (USGS appended): {minerals_successfully_harmonized}/{len(MINERALS)}")
    print(f"Rows added from USGS 2024 estimates: {total_usgs_rows_added}")

    print("\nUnmatched USGS countries by mineral (after country mapping):")
    for mineral in MINERALS:
        unmatched = unmatched_by_mineral.get(mineral, [])
        if unmatched:
            print(f"  {mineral}: {', '.join(unmatched)}")
        else:
            print(f"  {mineral}: None")

    if warnings_list:
        print("\nWarnings:")
        for warning in warnings_list:
            print(f"  - {warning}")

    print(f"\nSaved harmonized dataset: {output_long}")
    print(f"Saved harmonization summary: {output_summary}")


if __name__ == "__main__":
    main()
