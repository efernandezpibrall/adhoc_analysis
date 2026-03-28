"""
JKM-Brent Slope Historical Analysis

Fetches historical JKM (ICE_JKM_MO) and Brent (ICE_BRENT_FUTURES) prices from Enverus,
applies a year-based discount to JKM, and calculates the JKM/Brent slope.

Output: Excel file with columns:
    date, code1, contract1, value1, code2, contract2, value2,
    value1_discount, value1_with_discount, slope
"""

import os
import logging
import configparser
import datetime as dt
import pandas as pd
import numpy as np
import sqlalchemy
from trino.dbapi import connect
from trino.auth import JWTAuthentication

# --- Load Configuration from INI File ---
config_reader = configparser.ConfigParser(interpolation=None)
CONFIG_FILE_PATH = 'config.ini' # Assumes it's in the same directory

if not os.path.exists(CONFIG_FILE_PATH):
    logging.error(f"Configuration file not found: {CONFIG_FILE_PATH}")
    raise FileNotFoundError(f"Configuration file not found: {CONFIG_FILE_PATH}")

try:
    config_reader.read(CONFIG_FILE_PATH)

    # Read values from the ini file sections
    TRINOS_TOKEN = config_reader.get('TRINOS', 'TOKEN', fallback=None)
    TRINOS_USERNAME = config_reader.get('TRINOS', 'USERNAME', fallback=None)

    DB_CONNECTION_STRING = config_reader.get('DATABASE', 'CONNECTION_STRING', fallback=None)
    DB_SCHEMA = config_reader.get('DATABASE', 'SCHEMA', fallback='at_lng')

except (configparser.NoSectionError, configparser.NoOptionError) as e:
    logging.error(f"Error reading configuration file {CONFIG_FILE_PATH}: {e}", exc_info=True)
    raise ValueError(f"Missing section or option in {CONFIG_FILE_PATH}: {e}")

#postgres connection
engine = sqlalchemy.create_engine(DB_CONNECTION_STRING, pool_pre_ping=True)

# --- Trino Connection (Enverus) ---
conn_enverus = connect(
    host='trinolakehouse.adnoc.ae',
    port=443,
    user=TRINOS_USERNAME,
    auth=JWTAuthentication(TRINOS_TOKEN),
    http_scheme="https",
    verify=False,
    catalog="transformed",
    schema="enverus",
)


def read_table_conn(conn, query):
    """Execute query on Trino connection and return DataFrame."""
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    field_names = [i[0] for i in cur.description]
    df = pd.DataFrame(rows, columns=field_names)
    cur.close()
    return df


def get_enverus_prices(conn, code, category, version_name, from_COB, to_COB):
    """Fetch forward curve prices from Enverus data lake."""
    query = '''SELECT   code,
                        ondate AS COB,
                        currency,
                        units,
                        forward_curve_tenors_expiry AS expiry,
                        forward_curve_tenors_absolute AS contract,
                        forward_curve_tenors_value AS value
                        FROM enverus.curve
                        WHERE code='{}'
                            AND category='{}'
                            AND version_name='{}'
                            AND ondate_index >= {}
                            AND ondate_index <= {}
                            AND forward_curve_tenors_absolute NOT IN ('M-1','M-2','M-3')
                            AND forward_curve_tenors_value is not null
                        ORDER BY ondate, forward_curve_tenors_tenor
                            '''.format(code, category, version_name, from_COB, to_COB)

    df = read_table_conn(conn, query)
    df['COB'] = pd.to_datetime(df['COB'], format='%Y-%m-%d')
    df['contract_date'] = np.where(
        df['contract'] == 'SPOT',
        df['COB'],
        pd.to_datetime(df['contract'], format='%YM%m', errors='coerce')
    )
    df['expiry'] = pd.to_datetime(df['expiry'], format='%Y-%m-%d')
    return df


# --- Contract Pair Definitions ---
# Each tuple: (JKM contract, Brent contract = JKM + 1 month)
CONTRACT_PAIRS = [
    ('2026M04', '2026M05'),
    ('2026M06', '2026M07'),
    ('2026M09', '2026M10'),
    ('2026M12', '2027M01'),
    ('2027M02', '2027M03'),
    ('2027M04', '2027M05'),
    ('2028M01', '2028M02'),
    ('2028M03', '2028M04'),
    ('2028M05', '2028M06'),
    ('2028M07', '2028M08'),
    ('2028M09', '2028M10'),
    ('2028M11', '2028M12'),
    ('2029M01', '2029M02'),
    ('2029M03', '2029M04'),
    ('2029M05', '2029M06'),
    ('2029M07', '2029M08'),
    ('2029M08', '2029M09'),
    ('2029M09', '2029M10'),
    ('2030M01', '2030M02'),
    ('2030M03', '2030M04'),
    ('2030M05', '2030M06'),
    ('2030M07', '2030M08'),
    ('2030M09', '2030M10'),
    ('2030M12', '2031M01'),
]

# Year-based discount mapping
DISCOUNT_BY_YEAR = {
    2026: -0.69,
    2027: -0.71,
    2028: -0.90,
    2029: -0.92,
    2030: -0.92,
}

# --- Date Range ---
start_date = dt.datetime(2025, 12, 1)
end_date = dt.datetime.now()
from_COB = start_date.strftime("%Y%m%d")
to_COB = end_date.strftime("%Y%m%d")

print(f"Fetching data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

# --- Fetch JKM prices ---
print("Fetching ICE_JKM_MO prices from Enverus...")
df_jkm = get_enverus_prices(
    conn=conn_enverus,
    code='ICE_JKM_MO',
    category='FINANCIAL',
    version_name='FINAL',
    from_COB=from_COB,
    to_COB=to_COB,
)
jkm_contracts = [pair[0] for pair in CONTRACT_PAIRS]
df_jkm = df_jkm[df_jkm['contract'].isin(jkm_contracts)]
print(f"  JKM records: {len(df_jkm)} (across {df_jkm['contract'].nunique()} contracts)")

# --- Fetch Brent prices ---
print("Fetching ICE_BRENT_FUTURES prices from Enverus...")
df_brent = get_enverus_prices(
    conn=conn_enverus,
    code='ICE_BRENT_FUTURES',
    category='FINANCIAL',
    version_name='FINAL',
    from_COB=from_COB,
    to_COB=to_COB,
)
brent_contracts = [pair[1] for pair in CONTRACT_PAIRS]
df_brent = df_brent[df_brent['contract'].isin(brent_contracts)]
print(f"  Brent records: {len(df_brent)} (across {df_brent['contract'].nunique()} contracts)")

# --- Build mapping ---
df_mapping = pd.DataFrame(CONTRACT_PAIRS, columns=['contract1', 'contract2'])
# Extract year from JKM contract for discount lookup
df_mapping['year'] = df_mapping['contract1'].str[:4].astype(int)
df_mapping['value1_discount'] = df_mapping['year'].map(DISCOUNT_BY_YEAR)

# --- Prepare JKM side ---
df_jkm_slim = df_jkm[['COB', 'code', 'contract', 'value']].rename(columns={
    'COB': 'date',
    'code': 'code1',
    'contract': 'contract1',
    'value': 'value1',
})

# --- Prepare Brent side ---
df_brent_slim = df_brent[['COB', 'code', 'contract', 'value']].rename(columns={
    'COB': 'date',
    'code': 'code2',
    'contract': 'contract2',
    'value': 'value2',
})

# --- Merge JKM with mapping to get paired Brent contract ---
df_merged = df_jkm_slim.merge(df_mapping[['contract1', 'contract2', 'value1_discount']], on='contract1')

# --- Merge with Brent prices on date + contract2 ---
df_result = df_merged.merge(df_brent_slim, on=['date', 'contract2'], how='inner')

# --- Calculate derived columns ---
df_result['value1_with_discount'] = df_result['value1'] - df_result['value1_discount']
df_result['slope'] = df_result['value1_with_discount'] / df_result['value2']

# --- Select and order output columns ---
df_result = df_result[[
    'date', 'code1', 'contract1', 'value1',
    'code2', 'contract2', 'value2',
    'value1_discount', 'value1_with_discount', 'slope'
]].sort_values(['date', 'contract1']).reset_index(drop=True)

# --- Export to Excel ---
output_dir = os.path.dirname(__file__)
output_file = os.path.join(output_dir, 'jkm_brent_slope_analysis.xlsx')
df_result.to_excel(output_file, index=False, sheet_name='JKM_Brent_Slope')
print(f"\nResults exported to: {output_file}")
print(f"Total rows: {len(df_result)}")
print(f"Date range: {df_result['date'].min().strftime('%Y-%m-%d')} to {df_result['date'].max().strftime('%Y-%m-%d')}")
print(f"Contract pairs: {df_result['contract1'].nunique()}")
print(f"\nSample (first 5 rows):")
print(df_result.head().to_string(index=False))
