"""
UN Comtrade trade data downloader for CriMinGeo project.
Downloads bilateral trade flows (imports + exports) for critical minerals.

SETUP:
1. Register free at https://comtradeplus.un.org (use Google/Microsoft login)
2. Go to https://comtradedeveloper.un.org/ -> select "comtrade - v1" product
3. Copy your Primary Key (subscription key)
4. Paste it below in SUBSCRIPTION_KEY

FREE TIER LIMITS:
- 500 API calls per day
- Up to 100,000 records per call
- The script queries one HS code × one year per call
- Total calls needed: ~1,250 (50 HS codes × 25 years)
- So it takes ~3 days to download everything on the free tier
- The script saves progress and can be resumed

OUTPUT:
- One CSV per mineral in data/source/, e.g. UN_cobalt_trade.csv
- Each file has both primary (ore) and refined trade, with a 'stage' column
"""

import os
import time
import pandas as pd
import comtradeapicall

# ============================================================
# CONFIGURATION — edit these
# ============================================================

SUBSCRIPTION_KEY = "5b27262aea1f46458415e5f45b4fa9dc"  # <-- paste your free API key here

# Output directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "source")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Year range
START_YEAR = 2000
END_YEAR = 2024

# Progress file — tracks which (mineral, hs_code, year) combos are done
PROGRESS_FILE = os.path.join("data", "intermediate", "comtrade_download_progress.csv")

# Seconds to wait between API calls (free tier rate limit safety)
WAIT_SECONDS = 3.5

# ============================================================
# HS CODE BASKETS — from JRC RMIS 2024 dashboard
# Each mineral has primary and refined codes with descriptions
# ============================================================

MINERALS = {
    "cobalt": {
        "primary": {
            "260500": "Cobalt ores and concentrates",
        },
        "refined": {
            "282200": "Cobalt oxides and hydroxides",
            "810520": "Unwrought cobalt, mattes, intermediates, powders",
        },
    },
    "copper": {
        "primary": {
            "260300": "Copper ores and concentrates",
            "262030": "Slag, ash, residues containing copper",
            "740100": "Copper mattes, cement copper",
        },
        "refined": {
            "282550": "Copper oxides and hydroxides",
            "282741": "Copper chloride oxides and hydroxides",
            "283325": "Copper sulphates",
            "740200": "Unrefined copper, copper anodes",
            "740311": "Refined copper cathodes",
            "740312": "Refined copper wire-bars",
            "740313": "Refined copper billets",
            "740319": "Other refined unwrought copper",
            "740321": "Unwrought brass",
            "740322": "Unwrought bronze",
            "740329": "Other unwrought copper alloys",
            "740500": "Copper master alloys",
        },
    },
    "nickel": {
        "primary": {
            "260400": "Nickel ores and concentrates",
            "750110": "Nickel mattes",
            "750120": "Nickel oxide sinters and intermediates",
        },
        "refined": {
            "282540": "Nickel oxides and hydroxides",
            "282735": "Nickel chloride",
            "283324": "Nickel sulphate",
            "720260": "Ferronickel",
            "750210": "Unwrought nickel (cathodes etc.)",
            "750220": "Unwrought nickel alloys",
            "750400": "Nickel powders and flakes",
        },
    },
    "lithium": {
        "primary": {
            # No clean primary HS code — HS 253090 lumps too many minerals
        },
        "refined": {
            "282520": "Lithium oxide and hydroxide",
            "283691": "Lithium carbonate",
        },
    },
    "manganese": {
        "primary": {
            "260200": "Manganese ores and concentrates",
        },
        "refined": {
            "282010": "Manganese dioxide",
            "282090": "Other manganese oxides",
            "284161": "Potassium permanganate",
            "284169": "Other permanganates and manganates",
            "720211": "High-carbon ferromanganese",
            "720219": "Other ferromanganese",
            "720230": "Ferrosilicomanganese",
            "811100": "Unwrought manganese and articles",
        },
    },
    "tungsten": {
        "primary": {
            "261100": "Tungsten ores and concentrates",
        },
        "refined": {
            "284180": "Tungstates (wolframates)",
            "720280": "Ferrotungsten and ferrosilicotungsten",
            "810194": "Unwrought tungsten, bars/rods sintered",
            "810110": "Tungsten powders, articles, waste/scrap",
        },
    },
    "antimony": {
        "primary": {
            "261710": "Antimony ores and concentrates",
        },
        "refined": {
            "282580": "Antimony oxides",
            "811010": "Unwrought antimony, powders",
        },
    },
    "graphite": {
        "primary": {
            "250410": "Natural graphite in powder or flakes",
            "250490": "Natural graphite other forms",
        },
        "refined": {
            # No HS code exists for refined/spherical graphite
        },
    },
    "magnesium": {
        "primary": {
            # No HS code — dolomite/brines not allocated to Mg
        },
        "refined": {
            "810411": "Magnesium unwrought >=99.8%",
            "810419": "Other magnesium unwrought",
            "810430": "Magnesium powders and granules",
        },
    },
    "gallium": {
        "primary": {},
        "refined": {
            # HS 811292 lumps Ga with many other metals — pull as rough proxy
            "811292": "Gallium etc. unwrought (PROXY — lumped with other metals)",
        },
    },
    "germanium": {
        "primary": {},
        "refined": {
            # HS 282560 lumps Ge oxides with ZrO2 — pull as rough proxy
            "282560": "Germanium oxides and zirconium dioxide (PROXY — lumped)",
        },
    },
    "pgm": {
        "primary": {
            # HS 261690 lumps PGM ores with gold — not useful
        },
        "refined": {
            "711011": "Platinum unwrought or powder",
            "711021": "Palladium unwrought or powder",
            "711031": "Rhodium unwrought or powder",
            "711041": "Iridium, osmium, ruthenium unwrought or powder",
        },
    },
    "ree": {
        "primary": {
            "284690": "REE compounds/mixtures (excl. cerium) — PROXY",
        },
        "refined": {
            "280530": "REE metals, scandium, yttrium (intermixed/alloyed)",
            "284610": "Cerium compounds",
        },
    },
}


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def load_progress():
    """Load set of already-downloaded (mineral, hs_code, year) tuples."""
    if os.path.exists(PROGRESS_FILE):
        df = pd.read_csv(PROGRESS_FILE)
        return set(zip(df["mineral"], df["hs_code"], df["year"]))
    return set()


def save_progress(done_set):
    """Save progress to disk."""
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    records = [{"mineral": m, "hs_code": h, "year": y} for m, h, y in done_set]
    pd.DataFrame(records).to_csv(PROGRESS_FILE, index=False)


def fetch_one(hs_code, year):
    """
    Fetch bilateral trade for one HS code and one year.
    Returns a DataFrame or None if no data / error.
    """
    try:
        df = comtradeapicall.getFinalData(
            subscription_key=SUBSCRIPTION_KEY,
            typeCode="C",           # Commodities
            freqCode="A",           # Annual
            clCode="HS",            # HS classification
            period=str(year),
            reporterCode=None,      # All reporters
            cmdCode=str(hs_code),
            flowCode="M,X",         # Imports and exports
            partnerCode=None,       # All partners
            partner2Code=None,
            customsCode=None,
            motCode=None,
            maxRecords=100000,
            format_output="JSON",
            aggregateBy=None,
            breakdownMode="classic",
            countOnly=None,
            includeDesc=True,
        )
        if df is not None and len(df) > 0:
            return df
        return None
    except Exception as e:
        print(f"    ERROR fetching HS {hs_code} year {year}: {e}")
        return None


# ============================================================
# MAIN DOWNLOAD LOOP
# ============================================================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if SUBSCRIPTION_KEY == "YOUR_KEY_HERE":
        print("=" * 60)
        print("ERROR: Please set your UN Comtrade subscription key!")
        print("Register free at https://comtradeplus.un.org")
        print("Get your key at https://comtradedeveloper.un.org/")
        print("Then paste it in the SUBSCRIPTION_KEY variable.")
        print("=" * 60)
        return

    done = load_progress()
    years = list(range(START_YEAR, END_YEAR + 1))

    # Count total work
    total_calls = 0
    for mineral, stages in MINERALS.items():
        for stage_name, codes in stages.items():
            total_calls += len(codes) * len(years)
    remaining = total_calls - len(done)
    print(f"Total API calls needed: {total_calls}")
    print(f"Already done: {len(done)}")
    print(f"Remaining: {remaining}")
    print(f"Estimated time: {remaining * WAIT_SECONDS / 60:.0f} minutes")
    print(f"Free tier daily limit: 500 calls/day")
    if remaining > 500:
        print(f"  -> Will need ~{remaining // 500 + 1} days to complete")
    print()

    calls_today = 0

    for mineral, stages in MINERALS.items():
        mineral_data = []

        for stage_name, codes in stages.items():
            if not codes:
                continue

            for hs_code, hs_desc in codes.items():
                for year in years:
                    # Skip if already done
                    if (mineral, hs_code, str(year)) in done:
                        continue

                    # Check daily limit
                    if calls_today >= 490:
                        print(f"\n  Approaching daily limit ({calls_today} calls).")
                        print(f"  Saving progress and stopping.")
                        print(f"  Re-run the script tomorrow to continue.")
                        save_progress(done)
                        # Save whatever we have so far for this mineral
                        if mineral_data:
                            _save_mineral(mineral, mineral_data)
                        return

                    # Fetch
                    print(f"  [{calls_today+1}] {mineral} / {stage_name} / HS {hs_code} / {year}...", end=" ")
                    df = fetch_one(hs_code, year)

                    if df is not None and len(df) > 0:
                        df["mineral"] = mineral
                        df["stage"] = stage_name
                        df["hs_code_queried"] = hs_code
                        df["hs_description"] = hs_desc
                        mineral_data.append(df)
                        print(f"{len(df)} rows")
                    else:
                        print("no data")

                    # Track progress
                    done.add((mineral, hs_code, str(year)))
                    calls_today += 1

                    # Rate limit
                    time.sleep(WAIT_SECONDS)

                    # Save progress every 25 calls
                    if calls_today % 25 == 0:
                        save_progress(done)

        # Save this mineral's file
        if mineral_data:
            _save_mineral(mineral, mineral_data)

    # Final save
    save_progress(done)
    print("\nDone! All minerals downloaded.")


def _save_mineral(mineral, data_list):
    """Concatenate and save one mineral's trade data."""
    df = pd.concat(data_list, ignore_index=True)

    # Keep useful columns, drop internal API noise
    keep_cols = [
        "mineral", "stage", "hs_code_queried", "hs_description",
        "period", "reporterCode", "reporterISO", "reporterDesc",
        "partnerCode", "partnerISO", "partnerDesc",
        "flowCode", "flowDesc",
        "cmdCode", "cmdDesc",
        "primaryValue", "netWgt", "grossWgt", "qty", "qtyUnitAbbr",
        "cifvalue", "fobvalue",
    ]
    # Only keep columns that actually exist in the data
    existing = [c for c in keep_cols if c in df.columns]
    df_out = df[existing].copy()

    outpath = os.path.join(OUTPUT_DIR, f"UN_{mineral}_trade.csv")

    # If file already exists (from a previous partial run), append
    if os.path.exists(outpath):
        df_existing = pd.read_csv(outpath)
        df_out = pd.concat([df_existing, df_out], ignore_index=True)
        df_out = df_out.drop_duplicates()

    df_out.to_csv(outpath, index=False)
    print(f"  -> Saved {outpath} ({len(df_out)} rows)")


if __name__ == "__main__":
    main()
