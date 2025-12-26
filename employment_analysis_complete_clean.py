import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
import warnings
import os
warnings.filterwarnings('ignore')

MONGO_URI = 'mongodb+srv://shreyassudarshanam98_db_user:A3K4M9ln3cInfA5m@dubin.tdpbwgk.mongodb.net/'
DB_NAME = 'dublin_employment'
COLLECTION_RAW = 'employment_raw'
COLLECTION_CLEAN = 'employment_clean'
CSV_FILE = 'Business&Eco.csv'
OUTPUT_FOLDER = 'outputs'

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

client = None
db = None
try:
    client = MongoClient(MONGO_URI)
    client.admin.command('ping')
    db = client[DB_NAME]
except Exception as e:
    print(f"Database connection failed: {e}")
    exit(1)

raw_collection_exists = False
try:
    collection_names = db.list_collection_names()
    if COLLECTION_RAW in collection_names:
        row_count = db[COLLECTION_RAW].count_documents({})
        if row_count > 0:
            raw_collection_exists = True
except Exception as e:
    raw_collection_exists = False

if not raw_collection_exists:
    try:
        df_raw = pd.read_csv(CSV_FILE)
        records = df_raw.to_dict('records')
        result = db[COLLECTION_RAW].insert_many(records)
        uploaded_count = db[COLLECTION_RAW].count_documents({})
        if uploaded_count == 0:
            exit(1)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        exit(1)

try:
    documents = list(db[COLLECTION_RAW].find({}))
    df_raw = pd.DataFrame(documents)
    if '_id' in df_raw.columns:
        df_raw = df_raw.drop('_id', axis=1)
except Exception as e:
    print(f"Failed to fetch raw data: {e}")
    exit(1)

df_clean = df_raw.copy()
df_clean['year'] = df_clean['Quarterly'].str[:4].astype(int)
df_clean = df_clean.rename(columns={
    'NACE Rev 2 Economic Sector': 'sector',
    'Full and Part Time Status': 'employment_type',
    'VALUE': 'employment_count'
})
df_clean = df_clean[['year', 'sector', 'employment_type', 'employment_count']]

exclude_sectors = [
    'All NACE economic sectors',
    'Industry and Construction (B to F)',
    'Services (G to U)',
    'Not stated'
]
df_clean = df_clean[~df_clean['sector'].isin(exclude_sectors)]
df_clean['employment_count'] = pd.to_numeric(df_clean['employment_count'], errors='coerce')
df_clean = df_clean.dropna()
df_clean = df_clean[df_clean['employment_type'].isin(['Full-time', 'Part-time', 'All employment status'])]
df_clean['sector_short'] = df_clean['sector'].str.split('(').str[0].str.strip()

try:
    db[COLLECTION_CLEAN].drop()
    records = df_clean.to_dict('records')
    result = db[COLLECTION_CLEAN].insert_many(records)
    clean_count = db[COLLECTION_CLEAN].count_documents({})
    if clean_count == 0:
        exit(1)
except Exception as e:
    print(f"Upload to clean collection failed: {e}")
    exit(1)

try:
    documents = list(db[COLLECTION_CLEAN].find({}))
    df_from_db = pd.DataFrame(documents)
    if '_id' in df_from_db.columns:
        df_from_db = df_from_db.drop('_id', axis=1)
except Exception as e:
    print(f"Failed to fetch cleaned data: {e}")
    exit(1)

df_all_employment = df_from_db[df_from_db['employment_type'] == 'All employment status'].copy()
yearly_totals = df_all_employment.groupby(['year', 'sector_short'])['employment_count'].sum().reset_index()

min_year = df_from_db['year'].min()
max_year = df_from_db['year'].max()

first_year_data = yearly_totals[yearly_totals['year'] == min_year].set_index('sector_short')['employment_count']
last_year_data = yearly_totals[yearly_totals['year'] == max_year].set_index('sector_short')['employment_count']

growth_df = pd.DataFrame({
    'sector': first_year_data.index,
    'start_count': first_year_data.values,
    'end_count': last_year_data.values
})

growth_df['absolute_change'] = growth_df['end_count'] - growth_df['start_count']
growth_df['percent_change'] = ((growth_df['end_count'] - growth_df['start_count']) / growth_df['start_count']) * 100
growth_df = growth_df.sort_values('percent_change', ascending=False)

df_ft_pt = df_from_db[df_from_db['employment_type'].isin(['Full-time', 'Part-time'])].copy()
yearly_ft_pt = df_ft_pt.groupby(['year', 'employment_type'])['employment_count'].sum().reset_index()

ft_pt_pivot = yearly_ft_pt.pivot(index='year', columns='employment_type', values='employment_count').reset_index()
ft_pt_pivot['total'] = ft_pt_pivot['Full-time'] + ft_pt_pivot['Part-time']
ft_pt_pivot['pct_full_time'] = (ft_pt_pivot['Full-time'] / ft_pt_pivot['total']) * 100
ft_pt_pivot['pct_part_time'] = (ft_pt_pivot['Part-time'] / ft_pt_pivot['total']) * 100

plt.figure(figsize=(14, 8))
sectors = yearly_totals['sector_short'].unique()
colors = plt.cm.get_cmap('tab10')(np.linspace(0, 1, len(sectors)))

for idx, sector in enumerate(sectors):
    sector_data = yearly_totals[yearly_totals['sector_short'] == sector]
    plt.plot(sector_data['year'], sector_data['employment_count'], 
            marker='o', label=sector, linewidth=2, color=colors[idx])

plt.xlabel('Year', fontsize=12)
plt.ylabel('Employment Count (Thousands)', fontsize=12)
plt.title(f'Employment Trend by Sector ({min_year}–{max_year})', fontsize=14, fontweight='bold')
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(f'{OUTPUT_FOLDER}/plot_employment_trend.png', dpi=300, bbox_inches='tight')
plt.close()

plt.figure(figsize=(12, 7))
plt.fill_between(ft_pt_pivot['year'], 0, ft_pt_pivot['Full-time'], 
                 label='Full-time', alpha=0.7, color='#2ecc71')
plt.fill_between(ft_pt_pivot['year'], ft_pt_pivot['Full-time'], 
                 ft_pt_pivot['Full-time'] + ft_pt_pivot['Part-time'],
                 label='Part-time', alpha=0.7, color='#e74c3c')

first_pct = ft_pt_pivot.iloc[0]['pct_full_time']
last_pct = ft_pt_pivot.iloc[-1]['pct_full_time']

plt.text(min_year, ft_pt_pivot.iloc[0]['total'] * 0.5, 
         f'{min_year}: {first_pct:.0f}% full-time', 
         fontsize=10, bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
plt.text(max_year, ft_pt_pivot.iloc[-1]['total'] * 0.5, 
         f'{max_year}: {last_pct:.0f}% full-time', 
         fontsize=10, bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

plt.xlabel('Year', fontsize=12)
plt.ylabel('Employment Count (Thousands)', fontsize=12)
plt.title(f'Full-Time vs Part-Time Employment ({min_year}–{max_year})', fontsize=14, fontweight='bold')
plt.legend(loc='upper left', fontsize=11)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(f'{OUTPUT_FOLDER}/plot_fulltime_vs_parttime.png', dpi=300, bbox_inches='tight')
plt.close()

plt.figure(figsize=(12, 10))
colors = ['green' if x > 0 else 'red' for x in growth_df['percent_change']]
plt.barh(growth_df['sector'], growth_df['percent_change'], color=colors, alpha=0.7)
plt.xlabel('Growth Rate (%)', fontsize=12)
plt.ylabel('Sector', fontsize=12)
plt.title(f'Sector Growth Ranking ({min_year}–{max_year})', fontsize=14, fontweight='bold')
plt.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
plt.grid(True, alpha=0.3, axis='x')
plt.tight_layout()
plt.savefig(f'{OUTPUT_FOLDER}/plot_sector_growth.png', dpi=300, bbox_inches='tight')
plt.close()

print("EMPLOYMENT ANALYSIS RESULTS")

print("\nTOP 3 GROWING SECTORS:")
for idx, (i, row) in enumerate(growth_df.head(3).iterrows(), 1):
    print(f"  {idx}. {row['sector']}: +{row['percent_change']:.1f}% "
          f"({row['start_count']:.1f}k → {row['end_count']:.1f}k, +{row['absolute_change']:.1f}k)")

print("\nTOP 3 DECLINING SECTORS:")
for idx, (i, row) in enumerate(growth_df.tail(3).iloc[::-1].iterrows(), 1):
    print(f"  {idx}. {row['sector']}: {row['percent_change']:.1f}% "
          f"({row['start_count']:.1f}k → {row['end_count']:.1f}k, {row['absolute_change']:.1f}k)")

print("\nFULL-TIME vs PART-TIME TREND:")
first_pct_ft = ft_pt_pivot.iloc[0]['pct_full_time']
last_pct_ft = ft_pt_pivot.iloc[-1]['pct_full_time']
pct_change = last_pct_ft - first_pct_ft

print(f"  • {min_year}: {first_pct_ft:.1f}% full-time, {100-first_pct_ft:.1f}% part-time")
print(f"  • {max_year}: {last_pct_ft:.1f}% full-time, {100-last_pct_ft:.1f}% part-time")

if pct_change > 0:
    print(f"  • Full-time employment share increased by {pct_change:.1f} percentage points")
else:
    print(f"  • Full-time employment share decreased by {abs(pct_change):.1f} percentage points")

print("\nKEY INSIGHTS:")
top_sector = growth_df.iloc[0]
print(f"  1. {top_sector['sector']} showed the strongest growth at {top_sector['percent_change']:.1f}% "
      f"between {min_year} and {max_year}, adding {top_sector['absolute_change']:.1f}k jobs.")

worst_sector = growth_df.iloc[-1]
print(f"  2. {worst_sector['sector']} experienced the steepest decline at {worst_sector['percent_change']:.1f}%, "
      f"losing {abs(worst_sector['absolute_change']):.1f}k jobs.")

total_start = yearly_totals[yearly_totals['year'] == min_year]['employment_count'].sum()
total_end = yearly_totals[yearly_totals['year'] == max_year]['employment_count'].sum()
overall_growth = ((total_end - total_start) / total_start) * 100
print(f"  3. Overall employment grew by {overall_growth:.1f}% from {total_start:.1f}k to {total_end:.1f}k jobs.")

pt_growth = ((ft_pt_pivot.iloc[-1]['Part-time'] - ft_pt_pivot.iloc[0]['Part-time']) / 
             ft_pt_pivot.iloc[0]['Part-time']) * 100
print(f"  4. Part-time employment grew by {pt_growth:.1f}% over the period, indicating a shift "
      f"toward more flexible work arrangements.")

print(f"  5. The analysis covered {len(growth_df)} distinct economic sectors across {max_year - min_year + 1} years "
      f"of employment data.")

print("\n" + "="*80)
print(f"\nVisualizations saved to '{OUTPUT_FOLDER}/' folder")

if client:
    client.close()
