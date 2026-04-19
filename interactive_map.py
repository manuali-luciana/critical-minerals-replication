import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import os
import numpy as np
from datetime import datetime

# Get the script's directory and construct paths relative to it
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

st.set_page_config(page_title="Critical Minerals Production Map", layout="wide")

# ──────────────────────────────────────────────────────────
# Load and prepare data
# ──────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    """Load all BGS production data."""
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
        
        # Filter for mine production only
        if df['bgs_commodity_trans'].nunique() > 1:
            mine_mask = df['bgs_commodity_trans'].str.contains('mine|minerals|ore|primary|oxides', 
                                                               case=False, na=False)
            if mine_mask.any():
                df = df[mine_mask].copy()
        
        all_data[name] = df
    
    return all_data

@st.cache_data
def get_country_iso():
    """Map country names to ISO-3 codes for Plotly."""
    iso_mapping = {
        "China": "CHN", "Congo, Democratic Republic": "COD",
        "Australia": "AUS", "Chile": "CHL", "Indonesia": "IDN",
        "South Africa": "ZAF", "Russia": "RUS", "Myanmar": "MMR",
        "USA": "USA", "Brazil": "BRA", "Philippines": "PHL",
        "Canada": "CAN", "Tajikistan": "TJK", "Japan": "JPN",
        "Peru": "PER", "India": "IND", "Others": "OTH",
        "New Caledonia": "NCL", "Zimbabwe": "ZWE", "Mozambique": "MOZ",
        "Madagascar": "MDG", "Turkey": "TUR", "Thailand": "THA",
        "Bolivia": "BOL", "Argentina": "ARG", "Vietnam": "VNM",
        "Cuba": "CUB", "Zambia": "ZMB", "Korea (Rep. of)": "KOR",
        "Korea, Dem. P.R. of": "PRK", "Finland": "FIN",
        "Ukraine": "UKR", "Israel": "ISR", "Gabon": "GAB",
        "Mexico": "MEX", "Rwanda": "RWA", "Spain": "ESP",
        "Portugal": "PRT", "Colombia": "COL", "Papua New Guinea": "PNG",
        "North Korea": "PRK", "South Korea": "KOR",
    }
    return iso_mapping

def prepare_map_data(df, year, metric="share"):
    """Prepare data for choropleth map."""
    iso_map = get_country_iso()
    
    # Filter for the selected year
    year_data = df[df['year_clean'] == year].copy()
    
    # Group by country and sum quantities
    country_prod = year_data.groupby('country_trans')['quantity'].sum().reset_index()
    world_total = country_prod['quantity'].sum()
    
    if world_total == 0:
        return None
    
    # Add ISO codes
    country_prod['iso_alpha'] = country_prod['country_trans'].map(iso_map)
    country_prod = country_prod.dropna(subset=['iso_alpha'])
    
    # Calculate share
    country_prod['share'] = (country_prod['quantity'] / world_total * 100).round(2)
    
    if metric == "share":
        country_prod['value'] = country_prod['share']
        country_prod['label'] = country_prod['country_trans'] + "<br>" + \
                               country_prod['share'].astype(str) + "%"
    else:  # absolute
        country_prod['value'] = country_prod['quantity']
        unit = df['units'].iloc[0] if 'units' in df.columns else "tonnes"
        country_prod['label'] = country_prod['country_trans'] + "<br>" + \
                               country_prod['quantity'].apply(lambda x: f"{x:,.0f}").astype(str) + f" {unit}"
    
    return country_prod

def create_choropleth(mineral_data, mineral_name, year, metric="share"):
    """Create interactive choropleth map."""
    data = prepare_map_data(mineral_data, year, metric)
    
    if data is None or len(data) == 0:
        return None
    
    metric_title = "% Share of Global Production" if metric == "share" else "Production Volume"
    colorscale = "Viridis" if metric == "share" else "Blues"
    
    fig = go.Figure(data=go.Choropleth(
        locations=data['iso_alpha'],
        z=data['value'],
        customdata=data['country_trans'],
        text=data['label'],
        hovertemplate='<b>%{customdata}</b><br>' +
                      (f'Share: %{{z:.2f}}%<extra></extra>' if metric == "share" 
                       else f'Production: %{{z:,.0f}}<extra></extra>'),
        colorscale=colorscale,
        showscale=True,
        colorbar=dict(title=metric_title, thickness=15, len=0.7),
        marker_line_width=0.5,
    ))
    
    fig.update_layout(
        title_text=f"<b>{mineral_name} Production by Country ({year})</b><br>" + metric_title,
        title_font_size=16,
        geo=dict(
            showframe=False,
            showcoastlines=True,
            projection_type='natural earth',
            bgcolor='rgba(240, 240, 240, 0.5)',
        ),
        height=700,
        margin=dict(l=0, r=0, t=80, b=0),
        font=dict(family="Arial, sans-serif", size=11),
    )
    
    return fig

def compute_stats(df, year):
    """Compute summary statistics for the selected year."""
    year_data = df[df['year_clean'] == year].copy()
    country_prod = year_data.groupby('country_trans')['quantity'].sum().sort_values(ascending=False)
    total = country_prod.sum()
    
    if total == 0:
        return None
    
    # Top producers
    top3 = country_prod.head(3)
    hhi = ((country_prod / total * 100) ** 2).sum()
    
    return {
        'top1': top3.index[0] if len(top3) > 0 else "-",
        'top1_share': top3.iloc[0] / total * 100 if len(top3) > 0 else 0,
        'top3_share': top3.sum() / total * 100,
        'hhi': hhi,
        'num_producers': len(country_prod),
    }

# ──────────────────────────────────────────────────────────
# Streamlit UI
# ──────────────────────────────────────────────────────────

st.title("🌍 Critical Minerals Production Interactive Map")
st.markdown("""
Explore global mine production of critical minerals by country. 
Select a mineral and year to visualize production shares or absolute values.
""")

# Load data
all_data = load_data()
minerals = list(all_data.keys())

# Sidebar controls
st.sidebar.header("⚙️ Controls")
selected_mineral = st.sidebar.selectbox("Select Mineral:", minerals, index=0)
selected_metric = st.sidebar.radio("Display Metric:", ["% Share", "Absolute Production"], index=0)
metric_key = "share" if selected_metric == "% Share" else "absolute"

# Get available years for the selected mineral
available_years = sorted(all_data[selected_mineral]['year_clean'].dropna().unique())
max_year = int(available_years[-1]) if len(available_years) > 0 else 2023
min_year = int(available_years[0]) if len(available_years) > 0 else 1970

selected_year = st.sidebar.slider(
    "Select Year:",
    min_value=min_year,
    max_value=max_year,
    value=max_year,
    step=1
)

st.sidebar.markdown("---")
st.sidebar.markdown("📊 **Data Source:** BGS (British Geological Survey)")
st.sidebar.markdown(f"🔄 **Last updated:** March 2026")

# Create the map
col1, col2 = st.columns([3, 1])

with col1:
    fig = create_choropleth(all_data[selected_mineral], selected_mineral, selected_year, metric_key)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(f"No data available for {selected_mineral} in {selected_year}")

with col2:
    st.subheader("📈 Statistics")
    stats = compute_stats(all_data[selected_mineral], selected_year)
    
    if stats:
        st.metric("Top Producer", stats['top1'])
        st.metric("Top-1 Share", f"{stats['top1_share']:.1f}%")
        st.metric("Top-3 Share", f"{stats['top3_share']:.1f}%")
        st.metric("HHI Index", f"{stats['hhi']:.0f}")
        
        if stats['hhi'] > 2500:
            st.error("⚠️ Highly Concentrated (HHI > 2500)")
        elif stats['hhi'] > 1500:
            st.warning("⚠️ Moderately Concentrated (HHI > 1500)")
        else:
            st.success("✓ Competitive (HHI < 1500)")
        
        st.caption(f"Active producers: {stats['num_producers']}")
    else:
        st.warning("No data available")

# Time series for selected mineral
st.markdown("---")
st.subheader("📊 Global Production Trend (1970–2023)")

# Calculate world total by year
world_by_year = all_data[selected_mineral].groupby('year_clean')['quantity'].sum().reset_index()
world_by_year.columns = ['Year', 'Production']

fig_trend = go.Figure()
fig_trend.add_trace(go.Scatter(
    x=world_by_year['Year'],
    y=world_by_year['Production'],
    mode='lines+markers',
    name='Global Production',
    line=dict(color='#1f77b4', width=3),
    marker=dict(size=4),
    hovertemplate='<b>Year:</b> %{x}<br><b>Production:</b> %{y:,.0f}<extra></extra>'
))

fig_trend.update_layout(
    title=f"{selected_mineral} - Global Production Over Time",
    xaxis_title="Year",
    yaxis_title=f"Production ({all_data[selected_mineral]['units'].iloc[0] if 'units' in all_data[selected_mineral].columns else 'tonnes'})",
    hovermode='x unified',
    height=400,
    template='plotly_white',
    font=dict(family="Arial, sans-serif", size=11),
)

st.plotly_chart(fig_trend, use_container_width=True)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; font-size: 0.9em;'>
    <p><strong>Data Source:</strong> BGS British Geological Survey (1971–2023)</p>
    <p>Mine production only. When both mine and refined data available, mine production used to avoid double-counting.</p>
</div>
""", unsafe_allow_html=True)
