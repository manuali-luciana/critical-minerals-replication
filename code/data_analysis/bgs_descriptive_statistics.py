import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import os

# Get the script's directory and construct paths relative to it
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

plt.rcParams.update({
    'font.family': 'sans-serif', 'font.size': 10, 'figure.dpi': 150,
    'savefig.dpi': 150, 'savefig.pad_inches': 0.3,
    'axes.spines.top': False, 'axes.spines.right': False,
})

OUTPUT = os.path.join(PROJECT_ROOT, "results", "figures", "bgs_descriptive")
os.makedirs(OUTPUT, exist_ok=True)

# Enhanced color palette with better distinction for similar hues
COLORS = {
    "China": "#E63946", "Congo, Democratic Republic": "#1F77B4",
    "Australia": "#FFB703", "Chile": "#D62828", "Indonesia": "#FF006E",
    "South Africa": "#06A77D", "Russia": "#4361EE", "Myanmar": "#FFB403",
    "USA": "#1D3557", "Brazil": "#2A9D8F", "Philippines": "#023E8A",
    "Canada": "#A4013E", "Tajikistan": "#118B6F", "Japan": "#800000",
    "Peru": "#E76F51", "India": "#F77F00", "Others": "#CCCCCC",
    "New Caledonia": "#06A77D", "Zimbabwe": "#F9C74F", "Mozambique": "#52B788",
    "Madagascar": "#D62828", "Turkey": "#BC4B51", "Thailand": "#4A5859",
    "Bolivia": "#2D6A4F", "Argentina": "#457B9D", "Vietnam": "#C41E3A",
    "Cuba": "#003F5C", "Zambia": "#229954", "Korea (Rep. of)": "#00477C",
    "Korea, Dem. P.R. of": "#2E4053", "Finland": "#1B4965",
    "Ukraine": "#003D82", "Israel": "#0063DC", "Gabon": "#1FA535",
    "Mexico": "#165B33", "Rwanda": "#0096FF", "Spain": "#FFD60A",
    "Portugal": "#06A77D", "Colombia": "#FFD23F", "Papua New Guinea": "#E01E5A",
}
def gc(c): return COLORS.get(c, "#666666")

# ── Load all files ─────────────────────────────────────────
files = {
    "Antimony": os.path.join(PROJECT_ROOT, "data", "source", "BGS_antimony_production.csv"),
    "Cobalt": os.path.join(PROJECT_ROOT, "data", "source", "BGS_cobalt_production.csv"),
    "Copper": os.path.join(PROJECT_ROOT, "data", "source", "BGS_copper_production.csv"),
    "Gallium": os.path.join(PROJECT_ROOT, "data", "source", "BGS_gallium_production.csv"),
    "Germanium": os.path.join(PROJECT_ROOT, "data", "source", "BGS_germanium_production.csv"),
    "Graphite": os.path.join(PROJECT_ROOT, "data", "source", "BGS_graphite_production.csv"),
    "Lithium": os.path.join(PROJECT_ROOT, "data", "source", "BGS_lithium_production.csv"),
    "Magnesium": os.path.join(PROJECT_ROOT, "data", "source", "BGS_magnesium_production.csv"),
    "Manganese": os.path.join(PROJECT_ROOT, "data", "source", "BGS_manganese_production.csv"),
    "Nickel": os.path.join(PROJECT_ROOT, "data", "source", "BGS_nickel_production.csv"),
    "PGMs": os.path.join(PROJECT_ROOT, "data", "source", "BGS_platinum_production.csv"),
    "Rare Earths": os.path.join(PROJECT_ROOT, "data", "source", "BGS_REs_production.csv"),
    "Tungsten": os.path.join(PROJECT_ROOT, "data", "source", "BGS_tungsten_production.csv"),
}

all_data = {}
for name, path in files.items():
    df = pd.read_csv(path)
    df['year_clean'] = pd.to_datetime(df['year'], errors='coerce').dt.year
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    
    # For minerals with mine + refined, keep only mine production to avoid double counting
    # Exception: cobalt, nickel, copper — keep mine production only
    mine_keywords = ['mine', 'minerals', 'ore', 'primary', 'metal', 'oxides']
    if df['bgs_commodity_trans'].nunique() > 1:
        # Pick the "mine" variant
        mine_mask = df['bgs_commodity_trans'].str.contains('mine|minerals|ore|primary|oxides', case=False, na=False)
        if mine_mask.any():
            df = df[mine_mask].copy()
    
    all_data[name] = df

print("All files loaded successfully!")
for name, df in all_data.items():
    print(f"  {name}: {len(df)} rows, {df['year_clean'].min()}-{df['year_clean'].max()}, commodity: {df['bgs_commodity_trans'].unique()}")


# ── Helper functions ───────────────────────────────────────
def get_top_countries(df, year, n=6):
    recent = df[df['year_clean'] == year]
    top = recent.groupby('country_trans')['quantity'].sum().sort_values(ascending=False)
    return top.head(n).index.tolist()

def build_pivot(df, top_n=6):
    recent_year = df['year_clean'].max()
    top_countries = get_top_countries(df, recent_year, top_n)
    
    df2 = df.copy()
    df2['country_grouped'] = df2['country_trans'].apply(lambda c: c if c in top_countries else 'Others')
    agg = df2.groupby(['year_clean', 'country_grouped'])['quantity'].sum().reset_index()
    pivot = agg.pivot(index='year_clean', columns='country_grouped', values='quantity').fillna(0)
    
    col_order = [c for c in top_countries if c in pivot.columns]
    if 'Others' in pivot.columns:
        col_order.append('Others')
    pivot = pivot[col_order]
    pivot['World'] = pivot.sum(axis=1)
    return pivot

def compute_hhi(df):
    agg = df.groupby(['year_clean', 'country_trans'])['quantity'].sum().reset_index()
    agg = agg[agg['quantity'] > 0]
    world = agg.groupby('year_clean')['quantity'].sum().reset_index(name='world_total')
    merged = agg.merge(world, on='year_clean')
    merged['share'] = merged['quantity'] / merged['world_total'] * 100
    hhi = merged.groupby('year_clean').apply(lambda g: (g['share']**2).sum()).reset_index(name='HHI')
    return hhi

def china_share(df):
    agg = df.groupby(['year_clean', 'country_trans'])['quantity'].sum().reset_index()
    world = agg.groupby('year_clean')['quantity'].sum().reset_index(name='world_total')
    china = agg[agg['country_trans'] == 'China'].groupby('year_clean')['quantity'].sum().reset_index(name='china_total')
    merged = world.merge(china, on='year_clean', how='left').fillna(0)
    merged['china_pct'] = merged['china_total'] / merged['world_total'] * 100
    return merged


# ════════════════════════════════════════════════════════════
# FIGURE A: Individual stacked area charts — one per mineral
# ════════════════════════════════════════════════════════════
print("\nGenerating Figure A: Individual mineral stacked area charts...")

mineral_order = ["Rare Earths", "Lithium", "Cobalt", "Nickel", "Graphite",
                 "Copper", "Gallium", "Germanium", "Tungsten", "Antimony",
                 "PGMs", "Manganese", "Magnesium"]

for mineral in mineral_order:
    fig, ax = plt.subplots(figsize=(14, 7))
    df = all_data[mineral]
    pivot = build_pivot(df, top_n=6)
    
    countries = [c for c in pivot.columns if c != 'World']
    colors = [gc(c) for c in countries]
    
    ax.stackplot(pivot.index, [pivot[c] for c in countries],
                 labels=countries, colors=colors, alpha=0.85,
                 edgecolor='white', linewidth=0.5)
    
    ax.set_title(f"Global Mine Production of {mineral} by Country (BGS Data, 1971–2023)",
                 fontsize=13, fontweight='bold', pad=12)
    ax.legend(loc='upper left', fontsize=10, framealpha=0.9, handlelength=1.5)
    ax.set_xlabel("Year", fontsize=11)
    
    maxval = pivot['World'].max()
    if maxval > 1e6:
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x/1e6:.1f}M'))
        ax.set_ylabel("Production (Million tonnes)", fontsize=11)
    elif maxval > 10000:
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x/1000:.0f}k'))
        ax.set_ylabel("Production (Thousand tonnes)", fontsize=11)
    else:
        ax.set_ylabel("Production (tonnes)", fontsize=11)
    
    ax.tick_params(labelsize=9)
    ax.grid(axis='y', alpha=0.2, linestyle='--')
    
    plt.tight_layout()
    mineral_name = mineral.lower().replace(" ", "_")
    plt.savefig(os.path.join(OUTPUT, f"fig_{mineral_name}_stacked.png"), bbox_inches='tight', dpi=150)
    plt.close()

print(f"  ✓ Figure A: {len(mineral_order)} individual mineral charts saved")


# ════════════════════════════════════════════════════════════
# FIGURE B: China's mining share over time
# ════════════════════════════════════════════════════════════
print("Generating Figure B: China's mining share...")

fig, ax = plt.subplots(figsize=(15, 8))

mineral_colors = {
    "Rare Earths": "#DE2910", "Lithium": "#2196F3", "Cobalt": "#4CAF50",
    "Graphite": "#333333", "Tungsten": "#FF9800", "Antimony": "#9C27B0",
    "Gallium": "#F44336", "Germanium": "#E91E63", "Copper": "#795548",
    "Nickel": "#607D8B", "PGMs": "#00BCD4", "Manganese": "#8BC34A",
    "Magnesium": "#FF5722",
}

for mineral in mineral_order:
    cs = china_share(all_data[mineral])
    if cs['china_pct'].max() > 0:
        ax.plot(cs['year_clean'], cs['china_pct'], label=mineral,
                linewidth=2, color=mineral_colors.get(mineral, '#666'),
                alpha=0.85)

ax.axhline(y=50, color='gray', linestyle=':', alpha=0.4)
ax.axhline(y=65, color='#003399', linestyle='--', alpha=0.5, linewidth=1.2)
ax.text(2024, 66.5, 'EU CRM Act\n65% limit', fontsize=8, color='#003399', fontweight='bold')

ax.set_xlabel("Year", fontsize=12)
ax.set_ylabel("China's share of global mine production (%)", fontsize=12)
ax.set_ylim(0, 105)
ax.set_xlim(1970, 2025)
ax.legend(loc='upper left', fontsize=9, framealpha=0.9, ncol=2)
ax.set_title("China's Share of Global Mining Production (BGS Data, 1971–2023)\n"
             "Dramatic rise from the 1980s; diversification visible for REEs after 2010",
             fontsize=13, fontweight='bold', pad=12)

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT, "fig_china_share_timeseries.png"), bbox_inches='tight')
plt.close()
print("  ✓ Figure B saved")


# ════════════════════════════════════════════════════════════
# FIGURE C: HHI concentration over time
# ════════════════════════════════════════════════════════════
print("Generating Figure C: HHI time series...")

fig, ax = plt.subplots(figsize=(15, 8))

for mineral in mineral_order:
    hhi = compute_hhi(all_data[mineral])
    if len(hhi) > 3:
        # Smooth with rolling average for readability
        hhi_smooth = hhi.set_index('year_clean')['HHI'].rolling(3, min_periods=1, center=True).mean()
        ax.plot(hhi_smooth.index, hhi_smooth.values, label=mineral,
                linewidth=2, color=mineral_colors.get(mineral, '#666'), alpha=0.85)

ax.axhline(y=2500, color='orange', linestyle='--', alpha=0.6, linewidth=1.2)
ax.text(2024, 2700, 'Highly concentrated\n(DOJ: 2,500)', fontsize=8, color='orange')
ax.axhline(y=1500, color='#4CAF50', linestyle='--', alpha=0.4)

ax.set_xlabel("Year", fontsize=12)
ax.set_ylabel("Herfindahl-Hirschman Index (3-year rolling avg)", fontsize=12)
ax.set_xlim(1970, 2025)
ax.legend(loc='upper right', fontsize=9, framealpha=0.9, ncol=2)
ax.set_title("Mining Concentration Over Time (HHI, BGS Data)\n"
             "Rare earths peaked near 10,000 (near-monopoly) and are slowly declining; gallium remains extreme",
             fontsize=13, fontweight='bold', pad=12)

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT, "fig_hhi_timeseries.png"), bbox_inches='tight')
plt.close()
print("  ✓ Figure C saved")


# ════════════════════════════════════════════════════════════
# FIGURE D: Dumbbell — Top-1 producer share for each mineral (2023)
# ════════════════════════════════════════════════════════════
print("Generating Figure D: Top-1 share dumbbell (2023)...")

fig, ax = plt.subplots(figsize=(13, 8))

records = []
for mineral in mineral_order:
    df = all_data[mineral]
    yr = df['year_clean'].max()
    agg = df[df['year_clean'] == yr].groupby('country_trans')['quantity'].sum().sort_values(ascending=False)
    total = agg.sum()
    if total > 0:
        top1_country = agg.index[0]
        top1_share = agg.iloc[0] / total * 100
        top3_share = agg.head(3).sum() / total * 100
        records.append({
            'mineral': mineral, 'top1_country': top1_country,
            'top1_share': top1_share, 'top3_share': top3_share, 'year': yr
        })

rec_df = pd.DataFrame(records).sort_values('top1_share', ascending=True)
y_pos = np.arange(len(rec_df))

# Top-3 bars (background)
bars3 = ax.barh(y_pos, rec_df['top3_share'], height=0.5, color='#FFB81C', alpha=0.4,
                edgecolor='white', label='Top-3 countries share')

# Top-1 bars (foreground)
bars1 = ax.barh(y_pos, rec_df['top1_share'], height=0.5,
                color=[gc(c) for c in rec_df['top1_country']], edgecolor='white',
                label='Top-1 country share')

# Labels
for i, (_, row) in enumerate(rec_df.iterrows()):
    ax.text(row['top1_share'] + 1, i, f"{row['top1_country']} ({row['top1_share']:.0f}%)",
            va='center', fontsize=8.5, fontweight='bold',
            color=gc(row['top1_country']))

ax.set_yticks(y_pos)
ax.set_yticklabels(rec_df['mineral'], fontsize=10)
ax.set_xlabel("Share of global mine production (%)", fontsize=11)
ax.set_xlim(0, 110)

ax.axvline(x=65, color='#003399', linestyle='--', alpha=0.6, linewidth=1.5)
ax.text(66, len(rec_df) - 0.5, 'EU 65%\nlimit', fontsize=9, color='#003399', fontweight='bold')

ax.legend(loc='lower right', fontsize=10)
ax.set_title("Mining Concentration by Mineral (2023, BGS Data)\n"
             "Dark bar = top-1 country; light bar = top-3 countries combined",
             fontsize=13, fontweight='bold', pad=12)

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT, "fig_top1_concentration_2023.png"), bbox_inches='tight')
plt.close()
print("  ✓ Figure D saved")


# ════════════════════════════════════════════════════════════
# FIGURE E: Growth index (2000=100) — demand explosion
# ════════════════════════════════════════════════════════════
print("Generating Figure E: Production growth index...")

fig, ax = plt.subplots(figsize=(14, 8))

for mineral in mineral_order:
    df = all_data[mineral]
    world_by_year = df.groupby('year_clean')['quantity'].sum()
    
    # Use 2000 as base year
    if 2000 in world_by_year.index and world_by_year[2000] > 0:
        indexed = world_by_year / world_by_year[2000] * 100
        indexed = indexed[indexed.index >= 1995]
        ax.plot(indexed.index, indexed.values, label=mineral,
                linewidth=2.5, color=mineral_colors.get(mineral, '#666'), alpha=0.85)

ax.axhline(y=100, color='gray', linestyle='-', alpha=0.2)
ax.set_xlabel("Year", fontsize=12)
ax.set_ylabel("Production index (2000 = 100)", fontsize=12)
ax.set_xlim(1995, 2024)
ax.legend(loc='upper left', fontsize=9, framealpha=0.9, ncol=2)
ax.set_title("Global Mine Production Growth Since 2000 (BGS Data)\n"
             "Lithium and cobalt show explosive growth; copper and graphite grow steadily",
             fontsize=13, fontweight='bold', pad=12)

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT, "fig_growth_index.png"), bbox_inches='tight')
plt.close()
print("  ✓ Figure E saved")


# ════════════════════════════════════════════════════════════
# SUMMARY STATS TABLE
# ════════════════════════════════════════════════════════════
print("\nGenerating summary statistics...")

summary_rows = []
for mineral in mineral_order:
    df = all_data[mineral]
    yr = df['year_clean'].max()
    agg = df[df['year_clean'] == yr].groupby('country_trans')['quantity'].sum().sort_values(ascending=False)
    total = agg.sum()
    
    cs = china_share(df)
    china_2023 = cs[cs['year_clean'] == yr]['china_pct'].values
    china_pct = china_2023[0] if len(china_2023) > 0 else 0
    
    hhi = compute_hhi(df)
    hhi_2023 = hhi[hhi['year_clean'] == yr]['HHI'].values
    hhi_val = hhi_2023[0] if len(hhi_2023) > 0 else 0
    
    # Growth since 2000
    world_by_year = df.groupby('year_clean')['quantity'].sum()
    if 2000 in world_by_year.index and world_by_year[2000] > 0:
        growth = (world_by_year[yr] / world_by_year[2000] - 1) * 100
    else:
        growth = None
    
    summary_rows.append({
        'Mineral': mineral,
        'Data years': f"{df['year_clean'].min()}-{yr}",
        'Countries (2023)': len(agg),
        'Top-1 producer': agg.index[0] if len(agg) > 0 else '',
        'Top-1 share (%)': round(agg.iloc[0] / total * 100, 1) if total > 0 else 0,
        'China share (%)': round(china_pct, 1),
        'HHI (2023)': round(hhi_val, 0),
        'Growth since 2000 (%)': round(growth, 0) if growth is not None else 'N/A',
        'Unit': df['units'].iloc[0],
    })

summary_df = pd.DataFrame(summary_rows)
summary_df.to_csv(os.path.join(PROJECT_ROOT, "results", "tables", "summary_statistics.csv"), index=False)
print(summary_df.to_string(index=False))

print(f"\n{'='*60}")
print(f"All outputs saved to {OUTPUT}/")
print(f"Summary table saved to results/tables/summary_statistics.csv")
print(f"{'='*60}")
