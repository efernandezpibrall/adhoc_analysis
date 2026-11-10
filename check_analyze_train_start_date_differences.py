"""
Analyze differences between Woodmac estimated train start dates and actual volume start dates.

This script compares:
- Woodmac estimated start dates (from lng_train_date_start_est in woodmac_lng_plant_train)
- Actual start dates (first month with volume > 0 in woodmac_lng_plant_train_monthly_output_mta)

Outputs an Excel file with the differences for analysis.
"""

import pandas as pd
import configparser
from sqlalchemy import create_engine, text
from datetime import datetime

from pathlib import Path

# --- Load Configuration from INI File ---
CONFIG_FILE_PATH = Path(__file__).resolve().parents[1] / "config.ini"

config_reader = configparser.ConfigParser(interpolation=None)
config_reader.read(CONFIG_FILE_PATH)

DB_CONNECTION_STRING = config_reader.get('DATABASE', 'CONNECTION_STRING', fallback=None)
DB_SCHEMA = config_reader.get('DATABASE', 'SCHEMA', fallback='at_lng')

if not DB_CONNECTION_STRING:
    raise ValueError(f"Missing DATABASE CONNECTION_STRING in {CONFIG_FILE_PATH}")

# Create engine
engine = create_engine(DB_CONNECTION_STRING, pool_pre_ping=True)

print("="*80)
print("Analyzing Train Start Date Differences")
print("Woodmac Estimated vs Actual Volume Data")
print("="*80)

# Query to get the differences
query = text("""
WITH latest_trains AS (
    SELECT DISTINCT ON (id_plant, id_lng_train)
        id_plant,
        id_lng_train,
        lng_train_date_start_est,
        upload_timestamp_utc
    FROM at_lng.woodmac_lng_plant_train
    ORDER BY id_plant, id_lng_train, upload_timestamp_utc DESC
),
latest_plant_summary AS (
    SELECT DISTINCT ON (id_plant)
        id_plant,
        plant_name,
        country_name,
        upload_timestamp_utc
    FROM at_lng.woodmac_lng_plant_summary
    ORDER BY id_plant, upload_timestamp_utc DESC
),
latest_capacity AS (
    SELECT DISTINCT ON (id_plant, id_lng_train, year, month)
        id_plant,
        id_lng_train,
        year,
        month,
        metric_value,
        upload_timestamp_utc
    FROM at_lng.woodmac_lng_plant_monthly_capacity_nominal_mta
    ORDER BY id_plant, id_lng_train, year, month, upload_timestamp_utc DESC
),
capacity_max AS (
    SELECT
        id_plant,
        id_lng_train,
        MAX(metric_value) as max_capacity_mtpa
    FROM latest_capacity
    GROUP BY id_plant, id_lng_train
),
latest_output AS (
    SELECT DISTINCT ON (id_plant, id_lng_train, year, month)
        id_plant,
        id_lng_train,
        year,
        month,
        metric_value,
        upload_timestamp_utc
    FROM at_lng.woodmac_lng_plant_train_monthly_output_mta
    ORDER BY id_plant, id_lng_train, year, month, upload_timestamp_utc DESC
),
first_volume_date AS (
    SELECT
        id_plant,
        id_lng_train,
        MIN(TO_DATE(year || '-' || LPAD(month::text, 2, '0') || '-01', 'YYYY-MM-DD')) as actual_start_date
    FROM latest_output
    WHERE metric_value > 0
    GROUP BY id_plant, id_lng_train
)
SELECT
    ps.country_name,
    ps.plant_name,
    t.id_plant,
    t.id_lng_train,
    t.lng_train_date_start_est::date as woodmac_estimated_start,
    fvd.actual_start_date,
    CASE
        WHEN fvd.actual_start_date IS NULL THEN 'No volume data yet'
        WHEN t.lng_train_date_start_est::date = fvd.actual_start_date THEN 'Exact match'
        WHEN fvd.actual_start_date < t.lng_train_date_start_est::date THEN 'Started earlier'
        ELSE 'Started later'
    END as status,
    CASE
        WHEN fvd.actual_start_date IS NOT NULL AND t.lng_train_date_start_est IS NOT NULL
        THEN ABS((fvd.actual_start_date - t.lng_train_date_start_est::date)::integer)
        ELSE NULL
    END as days_difference,
    CASE
        WHEN fvd.actual_start_date IS NOT NULL AND t.lng_train_date_start_est IS NOT NULL
        THEN ROUND(ABS((fvd.actual_start_date - t.lng_train_date_start_est::date)::integer)::numeric / 30.44, 1)
        ELSE NULL
    END as months_difference,
    cm.max_capacity_mtpa,
    t.upload_timestamp_utc as woodmac_data_timestamp
FROM latest_trains t
JOIN latest_plant_summary ps ON t.id_plant = ps.id_plant
LEFT JOIN first_volume_date fvd ON t.id_plant = fvd.id_plant AND t.id_lng_train = fvd.id_lng_train
LEFT JOIN capacity_max cm ON t.id_plant = cm.id_plant AND t.id_lng_train = cm.id_lng_train
WHERE t.lng_train_date_start_est IS NOT NULL
ORDER BY
    CASE
        WHEN fvd.actual_start_date IS NULL THEN 0
        WHEN t.lng_train_date_start_est::date = fvd.actual_start_date THEN 1
        ELSE 2
    END,
    ABS((fvd.actual_start_date - t.lng_train_date_start_est::date)::integer) DESC NULLS LAST,
    ps.country_name,
    ps.plant_name
""")

print("\nExecuting query to fetch train start date differences...")
with engine.connect() as conn:
    result = conn.execute(query)
    rows = result.fetchall()
    columns = result.keys()

# Convert to DataFrame
df = pd.DataFrame(rows, columns=columns)

# Convert numeric columns to proper types
df['days_difference'] = pd.to_numeric(df['days_difference'], errors='coerce')
df['months_difference'] = pd.to_numeric(df['months_difference'], errors='coerce')
df['max_capacity_mtpa'] = pd.to_numeric(df['max_capacity_mtpa'], errors='coerce')

# Remove timezone from timestamp column for Excel compatibility
if 'woodmac_data_timestamp' in df.columns:
    df['woodmac_data_timestamp'] = pd.to_datetime(df['woodmac_data_timestamp']).dt.tz_localize(None)

print(f"Found {len(df)} trains with Woodmac estimated start dates")

# Summary statistics
print("\n" + "="*80)
print("SUMMARY STATISTICS")
print("="*80)

status_counts = df['status'].value_counts()
print("\nStatus breakdown:")
for status, count in status_counts.items():
    print(f"  {status}: {count} trains")

# Filter trains with differences
trains_with_diff = df[df['status'].isin(['Started earlier', 'Started later'])].copy()
print(f"\nTrains with date differences: {len(trains_with_diff)}")

if len(trains_with_diff) > 0:
    print(f"  Average difference: {trains_with_diff['months_difference'].mean():.1f} months")
    print(f"  Median difference: {trains_with_diff['months_difference'].median():.1f} months")
    print(f"  Max difference: {trains_with_diff['months_difference'].max():.1f} months")

    print("\nTop 10 trains with largest differences:")
    top_diff = trains_with_diff.nlargest(10, 'months_difference')[
        ['country_name', 'plant_name', 'woodmac_estimated_start', 'actual_start_date',
         'status', 'months_difference', 'max_capacity_mtpa']
    ]
    print(top_diff.to_string(index=False))

# Filter for trains starting in 2025 or later
df_future = df[
    (pd.to_datetime(df['woodmac_estimated_start']) >= '2025-01-01') |
    (pd.to_datetime(df['actual_start_date']) >= '2025-01-01')
].copy()

print(f"\n\nTrains with start dates in 2025 or later: {len(df_future)}")
if len(df_future) > 0:
    future_status = df_future['status'].value_counts()
    print("Status breakdown for 2025+ trains:")
    for status, count in future_status.items():
        print(f"  {status}: {count} trains")

# Generate output filename with timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = f'train_start_date_analysis_{timestamp}.xlsx'
output_dir = Path(__file__).resolve().parent / "outputs" / "lng" / "terminals"
output_dir.mkdir(parents=True, exist_ok=True)
output_path = output_dir / output_file

# Create Excel file with multiple sheets
print("\n" + "="*80)
print(f"Generating Excel file: {output_file}")
print("="*80)

with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    # Sheet 1: All trains
    df.to_excel(writer, sheet_name='All Trains', index=False)

    # Sheet 2: Trains with differences only
    if len(trains_with_diff) > 0:
        trains_with_diff.to_excel(writer, sheet_name='Differences Only', index=False)

    # Sheet 3: Future trains (2025+)
    if len(df_future) > 0:
        df_future.to_excel(writer, sheet_name='2025+ Trains', index=False)

    # Sheet 4: Summary statistics
    summary_data = {
        'Metric': [
            'Total trains analyzed',
            'Trains with exact match',
            'Trains that started earlier',
            'Trains that started later',
            'Trains with no volume data yet',
            'Trains with differences',
            'Average difference (months)',
            'Median difference (months)',
            'Max difference (months)',
            'Trains starting 2025+',
        ],
        'Value': [
            len(df),
            len(df[df['status'] == 'Exact match']),
            len(df[df['status'] == 'Started earlier']),
            len(df[df['status'] == 'Started later']),
            len(df[df['status'] == 'No volume data yet']),
            len(trains_with_diff),
            round(trains_with_diff['months_difference'].mean(), 1) if len(trains_with_diff) > 0 else 0,
            round(trains_with_diff['months_difference'].median(), 1) if len(trains_with_diff) > 0 else 0,
            round(trains_with_diff['months_difference'].max(), 1) if len(trains_with_diff) > 0 else 0,
            len(df_future),
        ]
    }
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_excel(writer, sheet_name='Summary', index=False)

print(f"\n✓ Excel file created successfully: {output_path}")
print("\nSheets created:")
print("  1. All Trains - Complete dataset with all trains")
print("  2. Differences Only - Trains where actual start differs from estimated")
print("  3. 2025+ Trains - Trains with start dates in 2025 or later")
print("  4. Summary - Statistics and summary information")

print("\n" + "="*80)
print("Analysis complete!")
print("="*80)
