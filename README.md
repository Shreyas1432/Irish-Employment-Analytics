# Irish-Employment-Analytics
ETL and analytics pipeline for Irish employment data (SQL &amp; Python)
## Overview
Analyzes 26 years of Irish employment data (1998–2024) across 15 economic sectors to identify workforce trends, sector growth patterns, and shifts between full-time and part-time employment. This matters for policy makers and businesses planning workforce strategies in response to economic changes.

## What I Built
- **ETL Pipeline**: Extracts CSV data, transforms sector names and employment types, loads into MongoDB Atlas
- **Analytics Engine**: Calculates year-over-year growth rates, employment type distributions, and sector rankings
- **Visualizations**: Three publication-ready charts showing employment trends, FT/PT ratios, and sector growth
- **Insight Generation**: Identifies top growing/declining sectors and workforce composition changes

## Tech Stack
Python (pandas, matplotlib, seaborn), MongoDB Atlas, NumPy

## Key Outcomes
- Professional/Scientific/Technical sectors showed strongest growth over 26 years
- Part-time employment increased significantly, indicating shift toward flexible work arrangements
- Manufacturing and traditional sectors experienced substantial declines
- Overall employment grew despite economic crises, with 10,000+ records analyzed

## How to Run
```bash
pip install -r requirements.txt
python employment_analysis_complete.py
```
Outputs save to `outputs/` folder. Requires MongoDB Atlas connection (credentials in script).
