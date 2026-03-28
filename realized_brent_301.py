"""
Realized Brent 301 Calculator

Calculates the historical realized Brent 301 index for each delivery month.
The Brent 301 is the arithmetic average of front-month Brent futures settlement
prices over the 3 consecutive months prior to the delivery month.

Output: Excel file with two sheets:
  - Summary: Realized 301 values per delivery month
  - Details: All daily prices used in calculations (for manual verification)

Usage:
    cd ~/development/Github
    conda activate dash_apps
    python3 realized_brent_301.py
"""

import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import configparser
from pathlib import Path
from sqlalchemy import create_engine

# Default config path (relative to this file's location)
DEFAULT_CONFIG_PATH = Path(__file__).parent / 'config.ini'

# ==============================================================================
# CONFIGURATION
# ==============================================================================

def get_config(config_path=None):
    """Load configuration from config.ini."""
    config = configparser.ConfigParser(interpolation=None)
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    config.read(path)
    return config


# ==============================================================================
# DATABASE FUNCTIONS - TRINO (PRIMARY)
# ==============================================================================

def get_trino_connection(config_path=None, catalog='raw', schema='ice_oil'):
    """Get Trino connection for ICE oil data access."""
    from trino.dbapi import connect
    from trino.auth import JWTAuthentication

    config = get_config(config_path)

    token = config.get('TRINOS', 'TOKEN', fallback=None)
    username = config.get('TRINOS', 'USERNAME', fallback=None)

    if not token or not username:
        raise ValueError(
            "TRINOS credentials not found in config.ini. "
            "Required: [TRINOS] TOKEN and USERNAME"
        )

    conn = connect(
        host='trinolakehouse.adnoc.ae',
        port=443,
        user=username,
        auth=JWTAuthentication(token),
        http_scheme='https',
        verify=False,
        catalog=catalog,
        schema=schema,
    )

    return conn


def read_trino_query(query, config_path=None, catalog='raw', schema='ice_oil'):
    """Execute a query against Trino and return results as DataFrame."""
    conn = get_trino_connection(config_path, catalog=catalog, schema=schema)

    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        field_names = [i[0] for i in cur.description]
        df = pd.DataFrame(rows, columns=field_names)
        cur.close()
        return df
    finally:
        conn.close()


# ==============================================================================
# DATABASE FUNCTIONS - POSTGRESQL (FALLBACK)
# ==============================================================================

def get_postgres_engine(config_path=None):
    """Get PostgreSQL engine for fallback data access."""
    config = get_config(config_path)
    connection_string = config['DATABASE']['CONNECTION_STRING']
    return create_engine(connection_string, pool_pre_ping=True)


def read_postgres_query(query, config_path=None):
    """Execute a query against PostgreSQL and return results as DataFrame."""
    engine = get_postgres_engine(config_path)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


# ==============================================================================
# DATA LOADING WITH FALLBACK
# ==============================================================================

def fetch_data_with_fallback(trino_query, postgres_query, config_path=None,
                              trino_catalog='raw', trino_schema='ice_oil'):
    """
    Fetch data trying Trino first, falling back to PostgreSQL on failure.
    """
    # Try Trino first
    try:
        df = read_trino_query(trino_query, config_path, catalog=trino_catalog, schema=trino_schema)
        print(f"Data fetched from Trino ({trino_catalog}.{trino_schema})")
        return df
    except Exception as trino_error:
        # Fallback to PostgreSQL
        print(f"Trino failed ({str(trino_error)[:100]}), falling back to PostgreSQL")
        try:
            df = read_postgres_query(postgres_query, config_path)
            print(f"Data fetched from PostgreSQL")
            return df
        except Exception as pg_error:
            raise Exception(
                f"Both Trino and PostgreSQL queries failed.\n"
                f"Trino error: {trino_error}\n"
                f"PostgreSQL error: {pg_error}"
            )


def load_brent_data():
    """Load Brent futures data from Trino with PostgreSQL fallback."""
    print("Loading Brent futures data...")

    # Trino query (primary source)
    trino_query = """
    SELECT
        trade_date,
        settlement_price,
        expiration_date,
        strip as maturity_date,
        contract
    FROM cleared_oil
    WHERE product = 'Brent Crude Futures'
      AND contract = 'B'
      AND settlement_price IS NOT NULL
    """

    # PostgreSQL fallback query
    postgres_query = """
    SELECT
        trade_date,
        settlement_price,
        expiration_date,
        strip as maturity_date,
        contract
    FROM at_lng.cleared_oil
    WHERE product = 'Brent Crude Futures'
      AND contract = 'B'
      AND settlement_price IS NOT NULL
    """

    df = fetch_data_with_fallback(trino_query, postgres_query)

    # Convert date columns
    df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
    df['expiration_date'] = pd.to_datetime(df['expiration_date'], errors='coerce')
    df['settlement_price'] = pd.to_numeric(df['settlement_price'], errors='coerce')

    # Drop rows with missing data
    df = df.dropna(subset=['trade_date', 'expiration_date', 'settlement_price'])

    print(f"Loaded {len(df):,} Brent price records")
    print(f"Date range: {df['trade_date'].min().strftime('%Y-%m-%d')} to {df['trade_date'].max().strftime('%Y-%m-%d')}")

    return df


# ==============================================================================
# FRONT-MONTH IDENTIFICATION
# ==============================================================================

def get_front_month_prices(df_brent):
    """
    For each trading day, identify and return the front-month contract price.
    Front-month = contract with nearest expiry that hasn't expired yet.

    Parameters:
        df_brent: DataFrame with columns [trade_date, settlement_price, expiration_date]

    Returns:
        DataFrame with one row per trade_date containing front-month price
    """
    df = df_brent.copy()

    # Filter to only contracts that haven't expired yet on each trade_date
    df = df[df['expiration_date'] >= df['trade_date']]

    if df.empty:
        print("Warning: No valid front-month data found")
        return pd.DataFrame()

    # For each trade_date, get the contract with minimum expiration_date (front-month)
    idx = df.groupby('trade_date')['expiration_date'].idxmin()
    front_month_df = df.loc[idx][['trade_date', 'settlement_price', 'expiration_date']].copy()

    front_month_df = front_month_df.reset_index(drop=True)
    front_month_df = front_month_df.sort_values('trade_date')

    print(f"Identified front-month prices for {len(front_month_df):,} trading days")

    return front_month_df


# ==============================================================================
# REALIZED 301 CALCULATION
# ==============================================================================

def calculate_realized_brent_301(df_front_month, delivery_month):
    """
    Calculate realized Brent 301 for a given delivery month.

    The 301 averages front-month Brent prices over the 3 months prior to delivery:
    - For April delivery: average of Jan, Feb, Mar prices

    Parameters:
        df_front_month: DataFrame with front-month prices per trade_date
        delivery_month: datetime - the delivery month (e.g., 2025-04-01)

    Returns:
        tuple: (avg_price, num_days, window_start, window_end)
    """
    # Calculate window: 3 months prior to delivery
    # For April delivery: window is Jan 1 to Mar 31
    window_start = (delivery_month - relativedelta(months=3)).replace(day=1)
    window_end = delivery_month.replace(day=1) - relativedelta(days=1)  # Last day of M-1

    # Filter front-month prices within the window
    mask = (
        (df_front_month['trade_date'] >= window_start) &
        (df_front_month['trade_date'] <= window_end)
    )
    window_data = df_front_month[mask]

    if window_data.empty:
        return None, 0, window_start, window_end

    avg_price = window_data['settlement_price'].mean()
    num_days = len(window_data)

    return avg_price, num_days, window_start, window_end


def get_delivery_months(df_brent):
    """
    Get all possible delivery months based on available data.

    A delivery month can be calculated if we have data covering the 3 months prior.
    """
    # Get the date range of available data
    min_date = df_brent['trade_date'].min()
    max_date = df_brent['trade_date'].max()

    # The earliest delivery month we can calculate is 3 months after min_date
    first_delivery = (min_date + relativedelta(months=4)).replace(day=1)

    # The latest delivery month is the month after max_date (partial data)
    last_delivery = (max_date + relativedelta(months=1)).replace(day=1)

    # Generate all months in range
    delivery_months = []
    current = first_delivery
    while current <= last_delivery:
        delivery_months.append(current)
        current = current + relativedelta(months=1)

    print(f"Will calculate 301 for {len(delivery_months)} delivery months")
    print(f"From {first_delivery.strftime('%Y-%m')} to {last_delivery.strftime('%Y-%m')}")

    return delivery_months


# ==============================================================================
# DETAIL RECORDS FOR AUDIT
# ==============================================================================

def build_detail_records(df_front_month, delivery_months):
    """
    Build detailed records showing which daily prices contribute to each 301.
    This allows manual verification of the calculations.

    Each row shows:
    - The trading date
    - The Brent price on that date
    - The front-month contract used
    - Which 301 delivery month this price contributes to
    """
    details = []

    for delivery_month in delivery_months:
        window_start = (delivery_month - relativedelta(months=3)).replace(day=1)
        window_end = delivery_month.replace(day=1) - relativedelta(days=1)

        mask = (
            (df_front_month['trade_date'] >= window_start) &
            (df_front_month['trade_date'] <= window_end)
        )
        window_data = df_front_month[mask].copy()

        if not window_data.empty:
            window_data['delivery_month_301'] = delivery_month.strftime('%Y-%m')
            details.append(window_data)

    if not details:
        return pd.DataFrame()

    df_details = pd.concat(details, ignore_index=True)

    # Format columns for output
    df_details = df_details.rename(columns={
        'trade_date': 'price_date',
        'settlement_price': 'brent_price',
        'expiration_date': 'contract_month'
    })

    # Format dates
    df_details['contract_month'] = pd.to_datetime(df_details['contract_month']).dt.strftime('%Y-%m')
    df_details['price_date'] = pd.to_datetime(df_details['price_date']).dt.strftime('%Y-%m-%d')

    # Reorder columns for clarity
    df_details = df_details[['price_date', 'brent_price', 'contract_month', 'delivery_month_301']]

    # Sort by delivery month, then by price date
    df_details = df_details.sort_values(['delivery_month_301', 'price_date'])

    return df_details


# ==============================================================================
# MAIN FUNCTION
# ==============================================================================

def main():
    """Main function to calculate realized Brent 301 and export to Excel."""
    print("=" * 60)
    print("Realized Brent 301 Calculator")
    print("=" * 60)

    # Load data from Trino
    df_brent = load_brent_data()

    if df_brent.empty:
        print("Error: No Brent data loaded. Exiting.")
        return

    # Get front-month prices
    print("\nIdentifying front-month contracts...")
    df_front_month = get_front_month_prices(df_brent)

    if df_front_month.empty:
        print("Error: Could not identify front-month prices. Exiting.")
        return

    # Get all delivery months
    print("\nDetermining delivery months...")
    delivery_months = get_delivery_months(df_brent)

    # Calculate realized 301 for all delivery months (Summary sheet)
    print("\nCalculating realized 301 for each delivery month...")
    summary_results = []
    for delivery_month in delivery_months:
        avg_price, num_days, start, end = calculate_realized_brent_301(df_front_month, delivery_month)

        # Determine status based on number of trading days
        # Typically ~63 trading days in 3 months, use 55 as threshold for "complete"
        if avg_price is None:
            status = 'no_data'
        elif num_days >= 55:
            status = 'complete'
        else:
            status = 'partial'

        summary_results.append({
            'delivery_month': delivery_month.strftime('%Y-%m'),
            'realized_brent_301': round(avg_price, 4) if avg_price else None,
            'trading_days': num_days,
            'window_start': start.strftime('%Y-%m-%d'),
            'window_end': end.strftime('%Y-%m-%d'),
            'status': status
        })

    df_summary = pd.DataFrame(summary_results)

    # Build detailed records (Details sheet)
    print("\nBuilding detail records for verification...")
    df_details = build_detail_records(df_front_month, delivery_months)

    # Export to Excel with two sheets
    output_file = f'realized_brent_301_{datetime.now().strftime("%Y%m%d")}.xlsx'

    print(f"\nExporting to {output_file}...")
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df_summary.to_excel(writer, sheet_name='Summary', index=False)
        df_details.to_excel(writer, sheet_name='Details', index=False)

    # Print summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total delivery months calculated: {len(df_summary)}")
    print(f"Complete calculations: {len(df_summary[df_summary['status'] == 'complete'])}")
    print(f"Partial calculations: {len(df_summary[df_summary['status'] == 'partial'])}")
    print(f"No data: {len(df_summary[df_summary['status'] == 'no_data'])}")
    print(f"\nDetail records for audit: {len(df_details):,} rows")
    print(f"\nOutput file: {output_file}")
    print("=" * 60)

    # Show sample of results
    print("\nSample of recent results:")
    print(df_summary.tail(10).to_string(index=False))


if __name__ == '__main__':
    main()
