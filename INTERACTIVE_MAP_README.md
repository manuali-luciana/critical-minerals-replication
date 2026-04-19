# Interactive Critical Minerals Production Map

## Features

✨ **Interactive World Map** — Color-coded by production share or absolute values  
📊 **Year & Mineral Selector** — Choose any year (1970–2023) and mineral  
🔢 **Real-time Statistics** — Top producers, concentration metrics (HHI)  
📈 **Production Trends** — Historical production over time  
🌐 **Online & Shareable** — Deploy to free Streamlit Cloud  

## Installation & Local Testing

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run Locally
From the `codes/data_analysis/` folder:
```bash
streamlit run interactive_map.py
```

This will open in your browser at `http://localhost:8501`

---

## Deploy to Streamlit Cloud (Free & Online)

### Quick Setup (5 minutes)

**Step 1: Push to GitHub**
- Create a GitHub account (free at github.com)
- Create a new repository
- Push your project folder to GitHub

**Step 2: Connect to Streamlit Cloud**
1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Click "Deploy an app"
3. Sign in with GitHub
4. Fill in:
   - **Repository:** `username/Critical-Minerals`
   - **Branch:** `main`
   - **File path:** `codes/data_analysis/interactive_map.py`
5. Click "Deploy"

Done! Your app will be live at:
```
https://your-username-critical-minerals.streamlit.app
```

You can share this link with anyone!

### Troubleshooting Deployment

If deployment fails because the data files aren't found:

1. **Make sure these files exist in your GitHub repo:**
   ```
   data/source/
   ├── BGS_antimony_production.csv
   ├── BGS_cobalt_production.csv
   ├── BGS_copper_production.csv
   ├── ... (all other CSV files)
   ```

2. **Check the requirements.txt is in the same folder** as `interactive_map.py`

---

## Features Explained

### 🗺️ **Choropleth Map**
- Darker colors = higher production
- Hover over countries to see exact values
- Toggle between % share and absolute production volume

### 📋 **Controls (Left Sidebar)**
- **Select Mineral** — Choose from 13 critical minerals
- **Display Metric** — % Share or Absolute Volume
- **Year Slider** — Jump to any year in the data

### 📊 **Statistics Panel (Right)**
- **Top Producer** — Leading country in selected year
- **Top-1 Share** — Percentage of global production
- **Top-3 Share** — Combined share of top 3 producers
- **HHI Index** — Market concentration (0=competitive, 10,000=monopoly)
  - \> 2,500: Highly concentrated ⚠️
  - 1,500–2,500: Moderately concentrated
  - < 1,500: Competitive ✓

### 📈 **Production Trend**
- Shows total global production over 50+ years
- Highlight growth, decline, or stability patterns

---

## Customization Ideas

Want to modify the app? Edit `interactive_map.py`:

- **Change colors:** Modify `colorscale="Viridis"` to other Plotly scales (Reds, Blues, Plasma, etc.)
- **Add more metrics:** Extend the statistics panel
- **Add country focus:** Show a selected country's production across all minerals
- **Download data:** Add CSV export button

---

## File Structure

```
codes/
└── data_analysis/
    ├── interactive_map.py       ← Streamlit app
    ├── requirements.txt         ← Dependencies
    ├── bgs_descriptive_statistics.py
    └── README.md               ← This file
```

---

## Support

If you encounter issues:

1. **Streamlit docs:** https://docs.streamlit.io
2. **Plotly Choropleth:** https://plotly.com/python/choropleth-maps
3. **Deploy docs:** https://docs.streamlit.io/streamlit-cloud/get-started

---

**Last Updated:** March 2026  
**Data Source:** BGS British Geological Survey (1971–2023)
