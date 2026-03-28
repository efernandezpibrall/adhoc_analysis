"""
Diagnostic Script: Arctic LNG-2 Misalignment Investigation
===========================================================
This script investigates the misalignment between LNG Train Start Dates
and Cumulative Monthly LNG Output by Train for Arctic LNG-2.
"""

import pandas as pd
import configparser
import os
from sqlalchemy import create_engine, text
from datetime import datetime

# --- Load Configuration ---
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    CONFIG_FILE_PATH = os.path.join(script_dir, 'config.ini')
except:
    CONFIG_FILE_PATH = 'config.ini'

config_reader = configparser.ConfigParser(interpolation=None)
config_reader.read(CONFIG_FILE_PATH)

DB_CONNECTION_STRING = config_reader.get('DATABASE', 'CONNECTION_STRING', fallback=None)
DB_SCHEMA = config_reader.get('DATABASE', 'SCHEMA', fallback=None)

if not DB_CONNECTION_STRING:
    raise ValueError(f"Missing DATABASE CONNECTION_STRING in {CONFIG_FILE_PATH}")

engine = create_engine(DB_CONNECTION_STRING, pool_pre_ping=True)

def execute_query(query, description):
    """Execute a query and return the dataframe"""
    print(f"\n{description}")
    print("-" * 80)
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
        return df
    except Exception as e:
        print(f"ERROR: {e}")
        return pd.DataFrame()

print("=" * 80)
print("ARCTIC LNG-2 DIAGNOSTIC INVESTIGATION")
print("=" * 80)
print()

# ============================================================================
# STEP 1: Check Arctic LNG-2 Train Metadata
# ============================================================================
print("\n" + "=" * 80)
print("STEP 1: Arctic LNG-2 Train Metadata")
print("=" * 80)

query_train_metadata = f"""
SELECT
    p.plant_name,
    p.country_name,
    t.id_plant,
    t.id_lng_train,
    t.lng_train_date_start_est,
    t.upload_timestamp_utc,
    t.lng_train_name
FROM {DB_SCHEMA}.woodmac_lng_plant_train t
JOIN {DB_SCHEMA}.woodmac_lng_plant_summary p ON t.id_plant = p.id_plant
WHERE p.plant_name ILIKE '%Arctic LNG%'
ORDER BY t.id_plant, t.id_lng_train, t.upload_timestamp_utc DESC
"""

df_train_metadata = execute_query(query_train_metadata, "Query: Arctic LNG-2 Train Metadata")
print(f"Found {len(df_train_metadata)} records")
if len(df_train_metadata) > 0:
    print(df_train_metadata.to_string())

# Get latest train data
query_latest_trains = f"""
SELECT DISTINCT ON (t.id_plant, t.id_lng_train)
    p.plant_name,
    p.country_name,
    t.id_plant,
    t.id_lng_train,
    t.lng_train_name,
    t.lng_train_date_start_est,
    t.upload_timestamp_utc
FROM {DB_SCHEMA}.woodmac_lng_plant_train t
JOIN {DB_SCHEMA}.woodmac_lng_plant_summary p ON t.id_plant = p.id_plant
WHERE p.plant_name ILIKE '%Arctic LNG%'
ORDER BY t.id_plant, t.id_lng_train, t.upload_timestamp_utc DESC
"""
df_latest_trains = execute_query(query_latest_trains, "\nLatest Train Data (DISTINCT ON)")
if len(df_latest_trains) > 0:
    print(df_latest_trains.to_string())

# ============================================================================
# STEP 2: Check Arctic LNG-2 Monthly Output Data
# ============================================================================
print("\n\n" + "=" * 80)
print("STEP 2: Arctic LNG-2 Monthly Output Data")
print("=" * 80)

query_monthly_output = f"""
SELECT
    p.plant_name,
    t.id_plant,
    t.id_lng_train,
    t.year,
    t.month,
    t.metric_value,
    t.upload_timestamp_utc,
    TO_DATE(t.year || '-' || LPAD(t.month::text, 2, '0') || '-01', 'YYYY-MM-DD') as date_constructed
FROM {DB_SCHEMA}.woodmac_lng_plant_train_monthly_output_mta t
JOIN {DB_SCHEMA}.woodmac_lng_plant_summary p ON t.id_plant = p.id_plant
WHERE p.plant_name ILIKE '%Arctic LNG%'
ORDER BY t.id_plant, t.id_lng_train, t.year, t.month, t.upload_timestamp_utc DESC
"""

df_monthly_output = execute_query(query_monthly_output, "Query: All Monthly Output Records")
print(f"Found {len(df_monthly_output)} records")
if len(df_monthly_output) > 0:
    print("\nFirst 20 records:")
    print(df_monthly_output.head(20).to_string())
    if len(df_monthly_output) > 20:
        print(f"\n... and {len(df_monthly_output) - 20} more records")

    print("\n\nSummary Statistics:")
    print("-" * 80)
    print(f"Date Range: {df_monthly_output['date_constructed'].min()} to {df_monthly_output['date_constructed'].max()}")
    print(f"Min Value: {df_monthly_output['metric_value'].min()}")
    print(f"Max Value: {df_monthly_output['metric_value'].max()}")
    print(f"Non-zero records: {(df_monthly_output['metric_value'] > 0).sum()}")
    print(f"Zero records: {(df_monthly_output['metric_value'] == 0).sum()}")
else:
    print("NO MONTHLY OUTPUT DATA FOUND!")

# Get latest monthly output (DISTINCT ON)
query_latest_monthly = f"""
SELECT DISTINCT ON (t.id_plant, t.id_lng_train, t.year, t.month)
    p.plant_name,
    t.id_plant,
    t.id_lng_train,
    t.year,
    t.month,
    t.metric_value,
    TO_DATE(t.year || '-' || LPAD(t.month::text, 2, '0') || '-01', 'YYYY-MM-DD') as date_constructed
FROM {DB_SCHEMA}.woodmac_lng_plant_train_monthly_output_mta t
JOIN {DB_SCHEMA}.woodmac_lng_plant_summary p ON t.id_plant = p.id_plant
WHERE p.plant_name ILIKE '%Arctic LNG%'
ORDER BY t.id_plant, t.id_lng_train, t.year, t.month, t.upload_timestamp_utc DESC
"""
df_latest_monthly = execute_query(query_latest_monthly, "\n\nLatest Monthly Output (DISTINCT ON year, month)")
print(f"Found {len(df_latest_monthly)} unique year-month combinations")
if len(df_latest_monthly) > 0:
    print(df_latest_monthly.to_string())

# Get first non-zero output date (as used in fetch_train_data)
query_first_nonzero = f"""
SELECT
    t.id_plant,
    t.id_lng_train,
    MIN(TO_DATE(t.year || '-' || LPAD(t.month::text, 2, '0') || '-01', 'YYYY-MM-DD')) as start_date
FROM (
    SELECT DISTINCT ON (id_plant, id_lng_train, year, month)
        id_plant, id_lng_train, year, month, metric_value
    FROM {DB_SCHEMA}.woodmac_lng_plant_train_monthly_output_mta t2
    JOIN {DB_SCHEMA}.woodmac_lng_plant_summary p ON t2.id_plant = p.id_plant
    WHERE p.plant_name ILIKE '%Arctic LNG%'
    ORDER BY id_plant, id_lng_train, year, month, upload_timestamp_utc DESC
) t
WHERE metric_value > 0
GROUP BY t.id_plant, t.id_lng_train
"""
df_first_nonzero = execute_query(query_first_nonzero, "\n\nFirst Non-Zero Output Date (as calculated in fetch_train_data)")
if len(df_first_nonzero) > 0:
    print(df_first_nonzero.to_string())
else:
    print("NO NON-ZERO OUTPUT FOUND!")

# ============================================================================
# STEP 3: Check Arctic LNG-2 Monthly Capacity Data
# ============================================================================
print("\n\n" + "=" * 80)
print("STEP 3: Arctic LNG-2 Monthly Capacity Data")
print("=" * 80)

query_monthly_capacity = f"""
SELECT
    p.plant_name,
    t.id_plant,
    t.id_lng_train,
    t.year,
    t.month,
    t.metric_value,
    t.upload_timestamp_utc
FROM {DB_SCHEMA}.woodmac_lng_plant_monthly_capacity_nominal_mta t
JOIN {DB_SCHEMA}.woodmac_lng_plant_summary p ON t.id_plant = p.id_plant
WHERE p.plant_name ILIKE '%Arctic LNG%'
ORDER BY t.id_plant, t.id_lng_train, t.year, t.month, t.upload_timestamp_utc DESC
"""

df_monthly_capacity = execute_query(query_monthly_capacity, "Query: Monthly Capacity Records")
print(f"Found {len(df_monthly_capacity)} records")
if len(df_monthly_capacity) > 0:
    print("\nFirst 20 records:")
    print(df_monthly_capacity.head(20).to_string())

    # Get max capacity per train
    query_max_capacity = f"""
    SELECT
        t.id_plant,
        t.id_lng_train,
        MAX(t.metric_value) as max_capacity
    FROM (
        SELECT DISTINCT ON (id_plant, id_lng_train, year, month)
            id_plant, id_lng_train, metric_value
        FROM {DB_SCHEMA}.woodmac_lng_plant_monthly_capacity_nominal_mta t2
        JOIN {DB_SCHEMA}.woodmac_lng_plant_summary p ON t2.id_plant = p.id_plant
        WHERE p.plant_name ILIKE '%Arctic LNG%'
        ORDER BY id_plant, id_lng_train, year, month, upload_timestamp_utc DESC
    ) t
    GROUP BY t.id_plant, t.id_lng_train
    """
    df_max_capacity = execute_query(query_max_capacity, "\n\nMax Capacity per Train")
    if len(df_max_capacity) > 0:
        print(df_max_capacity.to_string())
else:
    print("NO MONTHLY CAPACITY DATA FOUND!")

# ============================================================================
# STEP 4: Check Arctic LNG-2 Annual Output Data
# ============================================================================
print("\n\n" + "=" * 80)
print("STEP 4: Arctic LNG-2 Annual Output Data")
print("=" * 80)

query_annual_output = f"""
SELECT
    p.plant_name,
    t.id_plant,
    t.id_lng_train,
    t.year,
    t.metric_value,
    t.upload_timestamp_utc
FROM {DB_SCHEMA}.woodmac_lng_plant_train_annual_output_mta t
JOIN {DB_SCHEMA}.woodmac_lng_plant_summary p ON t.id_plant = p.id_plant
WHERE p.plant_name ILIKE '%Arctic LNG%'
ORDER BY t.id_plant, t.id_lng_train, t.year, t.upload_timestamp_utc DESC
"""

df_annual_output = execute_query(query_annual_output, "Query: Annual Output Records")
print(f"Found {len(df_annual_output)} records")
if len(df_annual_output) > 0:
    print(df_annual_output.to_string())

    # Get first non-zero annual output
    query_first_annual = f"""
    SELECT
        t.id_plant,
        t.id_lng_train,
        MIN(CASE WHEN t.metric_value > 0
            THEN TO_DATE(t.year || '-01-01', 'YYYY-MM-DD')
        END) as start_date
    FROM (
        SELECT DISTINCT ON (id_plant, id_lng_train, year)
            id_plant, id_lng_train, year, metric_value
        FROM {DB_SCHEMA}.woodmac_lng_plant_train_annual_output_mta t2
        JOIN {DB_SCHEMA}.woodmac_lng_plant_summary p ON t2.id_plant = p.id_plant
        WHERE p.plant_name ILIKE '%Arctic LNG%'
        ORDER BY id_plant, id_lng_train, year, upload_timestamp_utc DESC
    ) t
    GROUP BY t.id_plant, t.id_lng_train
    """
    df_first_annual = execute_query(query_first_annual, "\n\nFirst Non-Zero Annual Output")
    if len(df_first_annual) > 0:
        print(df_first_annual.to_string())
else:
    print("NO ANNUAL OUTPUT DATA FOUND!")

# ============================================================================
# STEP 5: Replicate fetch_train_data() Logic for Arctic LNG-2
# ============================================================================
print("\n\n" + "=" * 80)
print("STEP 5: Replicate fetch_train_data() Logic (base_view scenario)")
print("=" * 80)

query_replicate_train = f"""
WITH latest_trains AS (
    SELECT DISTINCT ON (id_plant, id_lng_train)
        id_plant,
        id_lng_train,
        lng_train_date_start_est
    FROM {DB_SCHEMA}.woodmac_lng_plant_train
    WHERE lng_train_date_start_est IS NOT NULL
    ORDER BY id_plant, id_lng_train, upload_timestamp_utc DESC
),
latest_plants AS (
    SELECT DISTINCT ON (id_plant)
        id_plant,
        plant_name,
        country_name
    FROM {DB_SCHEMA}.woodmac_lng_plant_summary
    ORDER BY id_plant, upload_timestamp_utc DESC
),
monthly_capacity AS (
    SELECT
        id_plant,
        id_lng_train,
        MAX(metric_value) as max_capacity
    FROM (
        SELECT DISTINCT ON (id_plant, id_lng_train, year, month)
            id_plant, id_lng_train, metric_value
        FROM {DB_SCHEMA}.woodmac_lng_plant_monthly_capacity_nominal_mta
        ORDER BY id_plant, id_lng_train, year, month, upload_timestamp_utc DESC
    ) c
    GROUP BY id_plant, id_lng_train
),
monthly_start_dates AS (
    SELECT
        id_plant,
        id_lng_train,
        MIN(TO_DATE(year || '-' || LPAD(month::text, 2, '0') || '-01', 'YYYY-MM-DD')) as start_date
    FROM (
        SELECT DISTINCT ON (id_plant, id_lng_train, year, month)
            id_plant, id_lng_train, year, month, metric_value
        FROM {DB_SCHEMA}.woodmac_lng_plant_train_monthly_output_mta
        ORDER BY id_plant, id_lng_train, year, month, upload_timestamp_utc DESC
    ) o
    WHERE metric_value > 0
    GROUP BY id_plant, id_lng_train
),
trains_with_monthly AS (
    SELECT DISTINCT id_plant, id_lng_train FROM monthly_capacity
    UNION
    SELECT DISTINCT id_plant, id_lng_train FROM monthly_start_dates
),
annual_data AS (
    SELECT
        a.id_plant,
        a.id_lng_train,
        MAX(a.metric_value) as max_capacity,
        MIN(CASE WHEN a.metric_value > 0
            THEN TO_DATE(a.year || '-01-01', 'YYYY-MM-DD')
        END) as start_date
    FROM (
        SELECT DISTINCT ON (id_plant, id_lng_train, year)
            id_plant, id_lng_train, year, metric_value
        FROM {DB_SCHEMA}.woodmac_lng_plant_train_annual_output_mta
        ORDER BY id_plant, id_lng_train, year, upload_timestamp_utc DESC
    ) a
    WHERE NOT EXISTS (
        SELECT 1 FROM trains_with_monthly m
        WHERE m.id_plant = a.id_plant AND m.id_lng_train = a.id_lng_train
    )
    GROUP BY a.id_plant, a.id_lng_train
)
SELECT
    p.plant_name,
    p.country_name,
    t.id_plant,
    t.id_lng_train,
    COALESCE(msd.start_date, ad.start_date, t.lng_train_date_start_est::date) as final_start_date,
    msd.start_date as monthly_derived_start,
    ad.start_date as annual_derived_start,
    t.lng_train_date_start_est::date as woodmac_original_start,
    COALESCE(mc.max_capacity, ad.max_capacity) as capacity,
    CASE
        WHEN msd.start_date IS NOT NULL THEN 'monthly_output'
        WHEN ad.start_date IS NOT NULL THEN 'annual_output'
        ELSE 'woodmac_estimate'
    END as start_date_source
FROM latest_trains t
JOIN latest_plants p ON t.id_plant = p.id_plant
LEFT JOIN monthly_capacity mc ON t.id_plant = mc.id_plant AND t.id_lng_train = mc.id_lng_train
LEFT JOIN monthly_start_dates msd ON t.id_plant = msd.id_plant AND t.id_lng_train = msd.id_lng_train
LEFT JOIN annual_data ad ON t.id_plant = ad.id_plant AND t.id_lng_train = ad.id_lng_train
WHERE p.plant_name ILIKE '%Arctic LNG%'
  AND COALESCE(mc.max_capacity, ad.max_capacity) IS NOT NULL
ORDER BY p.country_name, p.plant_name, COALESCE(msd.start_date, ad.start_date, t.lng_train_date_start_est::date)
"""

df_replicate_train = execute_query(query_replicate_train, "Query: Replicate fetch_train_data() for Arctic LNG-2")
if len(df_replicate_train) > 0:
    print(df_replicate_train.to_string())

# ============================================================================
# STEP 6: Summary and Analysis
# ============================================================================
print("\n\n" + "=" * 80)
print("SUMMARY AND ANALYSIS")
print("=" * 80)

print("\n1. TRAIN METADATA:")
print("-" * 80)
if len(df_latest_trains) > 0:
    for _, train in df_latest_trains.iterrows():
        print(f"   Train {train['id_lng_train']}: Woodmac Start Date = {train['lng_train_date_start_est']}")
else:
    print("   No train metadata found!")

print("\n2. MONTHLY OUTPUT DATA:")
print("-" * 80)
if len(df_latest_monthly) > 0:
    print(f"   Total unique year-month combinations: {len(df_latest_monthly)}")
    print(f"   Non-zero records: {(df_latest_monthly['metric_value'] > 0).sum()}")
    if (df_latest_monthly['metric_value'] > 0).sum() > 0:
        first_nonzero = df_latest_monthly[df_latest_monthly['metric_value'] > 0]['date_constructed'].min()
        print(f"   First non-zero output month: {first_nonzero}")
    else:
        print("   NO NON-ZERO OUTPUT FOUND!")
else:
    print("   No monthly output data found!")

print("\n3. FETCH_TRAIN_DATA() RESULT:")
print("-" * 80)
if len(df_replicate_train) > 0:
    for _, train in df_replicate_train.iterrows():
        print(f"   Train {train['id_lng_train']}:")
        print(f"      Final Start Date: {train['final_start_date']}")
        print(f"      Source: {train['start_date_source']}")
        print(f"      Monthly Derived: {train['monthly_derived_start']}")
        print(f"      Annual Derived: {train['annual_derived_start']}")
        print(f"      Woodmac Original: {train['woodmac_original_start']}")
        print(f"      Capacity: {train['capacity']}")
else:
    print("   No trains returned by fetch_train_data() logic!")

print("\n4. ROOT CAUSE ANALYSIS:")
print("-" * 80)
if len(df_latest_trains) > 0 and len(df_latest_monthly) > 0:
    has_nonzero = (df_latest_monthly['metric_value'] > 0).sum() > 0
    if not has_nonzero:
        print("   ❌ ISSUE IDENTIFIED: Arctic LNG-2 has train metadata with start dates,")
        print("      but NO non-zero monthly output data!")
        print("   ")
        print("   This means:")
        print("   - Timeline chart shows the train bar (from metadata start date)")
        print("   - Volume chart filters out all months (line 747: pivot_df[pivot_df.sum(axis=1) > 0])")
        print("   - Result: MISALIGNMENT between the two charts")
    else:
        train_start = df_latest_trains.iloc[0]['lng_train_date_start_est']
        first_output = df_latest_monthly[df_latest_monthly['metric_value'] > 0]['date_constructed'].min()
        if pd.notna(train_start) and pd.notna(first_output):
            train_start_date = pd.to_datetime(train_start).date()
            first_output_date = pd.to_datetime(first_output).date()
            if train_start_date != first_output_date:
                print(f"   ❌ ISSUE IDENTIFIED: Start date mismatch!")
                print(f"      Train metadata start: {train_start_date}")
                print(f"      First non-zero output: {first_output_date}")
                print(f"      Difference: {(first_output_date - train_start_date).days} days")
            else:
                print("   ✓ Start dates match between train metadata and output data")
else:
    print("   Insufficient data to perform root cause analysis")

print("\n\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
