"""
Historical Q1-Q2 Spread Analysis

Finds the historical maximum/minimum spread between Q1 and Q2 contracts (same year).
- Always calculates Q1-Q2 spread for the next calendar year
- From Jan 1 onwards, calculates spread for the following year
- Requires complete 3-month data for both Q1 and Q2
- Calculates simple average of 3 monthly contracts for each quarter

Change CONTRACT parameter below to switch commodities.
"""

import configparser
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from sqlalchemy import create_engine
from datetime import date

# =============================================================================
# CONFIGURATION - Change this to switch commodities
# =============================================================================
CONTRACT = 'TFM'  # Options: 'TFM' (TTF), 'H' (HH), 'JKM', 'IGA' (PSV)

# Contract name mapping for display
CONTRACT_NAMES = {
    'TFM': 'TTF',
    'H': 'Henry Hub',
    'JKM': 'JKM',
    'IGA': 'PSV'
}

# Default config path (relative to this file's location)
DEFAULT_CONFIG_PATH = Path(__file__).parent / 'config.ini'


def get_config(config_path=None):
    """Load configuration from config.ini."""
    config = configparser.ConfigParser(interpolation=None)
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    config.read(path)
    return config


def get_trino_connection(config_path=None, catalog='raw', schema='ice_gas'):
    """Get Trino connection for ICE settlement data access."""
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


def read_trino_query(query, config_path=None, catalog='raw', schema='ice_gas'):
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


def fetch_data_with_fallback(trino_query, postgres_query, config_path=None,
                              trino_catalog='raw', trino_schema='ice_gas'):
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


def get_next_q1_q2_year(trade_date: date) -> int:
    """
    Determine the year for the next Q1-Q2 spread based on trade date.

    Logic:
    - Throughout the entire year, look at next year's Q1-Q2
    - Example: In 2025, always calculate Q1-Q2 2026
    - From Jan 1, 2026, calculate Q1-Q2 2027

    Returns:
        year: Year for both Q1 and Q2 contracts
    """
    return trade_date.year + 1


def get_quarter_months(quarter: str) -> list:
    """Get the months for a quarter."""
    if quarter == 'Q1':
        return [1, 2, 3]  # January, February, March
    elif quarter == 'Q2':
        return [4, 5, 6]  # April, May, June
    elif quarter == 'Q3':
        return [7, 8, 9]  # July, August, September
    elif quarter == 'Q4':
        return [10, 11, 12]  # October, November, December
    else:
        raise ValueError(f"Unsupported quarter: {quarter}")


def load_settlement_data(contract: str) -> pd.DataFrame:
    """Load all settlement data for the specified contract using Trino with PostgreSQL fallback."""
    # Trino query (primary source)
    trino_query = f"""
    SELECT
        trade_date,
        strip,
        settlement_price,
        hub
    FROM cleared_gas
    WHERE contract = '{contract}'
      AND strike IS NULL
    ORDER BY trade_date, strip
    """

    # PostgreSQL fallback query
    postgres_query = f"""
    SELECT
        trade_date,
        strip,
        settlement_price,
        hub
    FROM at_lng.cleared_gas
    WHERE contract = '{contract}'
    ORDER BY trade_date, strip
    """

    df = fetch_data_with_fallback(trino_query, postgres_query)

    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
    df['strip'] = pd.to_datetime(df['strip']).dt.date
    df['settlement_price'] = df['settlement_price'].astype(float)
    return df


def calculate_q1_q2_spread(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Q1-Q2 spread for each trade date.
    Q1 and Q2 are always from the same year (next year).
    Only includes spreads where all 6 months (3 for Q1 + 3 for Q2) are available.
    """
    results = []

    # Group by trade date
    for trade_date, group in df.groupby('trade_date'):
        spread_year = get_next_q1_q2_year(trade_date)

        # Calculate Q1 price
        q1_months = get_quarter_months('Q1')
        q1_strips = [date(spread_year, m, 1) for m in q1_months]
        q1_prices = group[group['strip'].isin(q1_strips)]['settlement_price'].values

        # Calculate Q2 price
        q2_months = get_quarter_months('Q2')
        q2_strips = [date(spread_year, m, 1) for m in q2_months]
        q2_prices = group[group['strip'].isin(q2_strips)]['settlement_price'].values

        # Only calculate spread if both quarters have complete data
        if len(q1_prices) == 3 and len(q2_prices) == 3:
            q1_avg = sum(q1_prices) / 3
            q2_avg = sum(q2_prices) / 3
            spread = q1_avg - q2_avg  # Q1 minus Q2

            results.append({
                'trade_date': trade_date,
                'contract_year': spread_year,
                'q1_price': q1_avg,
                'q2_price': q2_avg,
                'spread': spread,
                'spread_label': f"Q1-Q2 {spread_year}"
            })

    return pd.DataFrame(results)


def find_historical_extremes(spread_df: pd.DataFrame) -> dict:
    """Find the historical maximum and minimum for Q1-Q2 spread."""
    results = {}

    if not spread_df.empty:
        # Maximum spread (Q1 most expensive vs Q2)
        max_idx = spread_df['spread'].idxmax()
        max_row = spread_df.loc[max_idx]
        results['max'] = {
            'spread': max_row['spread'],
            'trade_date': max_row['trade_date'],
            'spread_label': max_row['spread_label'],
            'contract_year': max_row['contract_year'],
            'q1_price': max_row['q1_price'],
            'q2_price': max_row['q2_price']
        }

        # Minimum spread (Q2 most expensive vs Q1, or smallest backwardation)
        min_idx = spread_df['spread'].idxmin()
        min_row = spread_df.loc[min_idx]
        results['min'] = {
            'spread': min_row['spread'],
            'trade_date': min_row['trade_date'],
            'spread_label': min_row['spread_label'],
            'contract_year': min_row['contract_year'],
            'q1_price': min_row['q1_price'],
            'q2_price': min_row['q2_price']
        }

    return results


def plot_spread(spread_df: pd.DataFrame, extremes: dict, contract_name: str):
    """Create time series chart for Q1-Q2 spread."""
    fig, ax = plt.subplots(figsize=(14, 7))

    spread_data = spread_df.sort_values('trade_date').copy()
    spread_data['trade_date_dt'] = pd.to_datetime(spread_data['trade_date'])

    # Plot spread line
    ax.plot(spread_data['trade_date_dt'], spread_data['spread'],
            color='#9b59b6', linewidth=1.5, label='Q1-Q2 Spread')

    # Fill positive (backwardation) and negative (contango) differently
    ax.fill_between(spread_data['trade_date_dt'], spread_data['spread'], 0,
                   where=(spread_data['spread'] >= 0),
                   alpha=0.3, color='#e74c3c', label='Backwardation (Q1 > Q2)')
    ax.fill_between(spread_data['trade_date_dt'], spread_data['spread'], 0,
                   where=(spread_data['spread'] < 0),
                   alpha=0.3, color='#3498db', label='Contango (Q1 < Q2)')

    # Mark the maximum
    if 'max' in extremes:
        max_data = extremes['max']
        max_date = pd.to_datetime(max_data['trade_date'])
        ax.scatter([max_date], [max_data['spread']],
                  color='red', s=100, zorder=5, marker='*')
        ax.annotate(f"Max: {max_data['spread']:.2f}\n{max_data['trade_date']}\n({max_data['spread_label']})",
                   xy=(max_date, max_data['spread']),
                   xytext=(15, 15), textcoords='offset points',
                   fontsize=9, color='red',
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='red', alpha=0.8),
                   arrowprops=dict(arrowstyle='->', color='red'))

    # Mark the minimum
    if 'min' in extremes:
        min_data = extremes['min']
        min_date = pd.to_datetime(min_data['trade_date'])
        ax.scatter([min_date], [min_data['spread']],
                  color='blue', s=100, zorder=5, marker='*')
        ax.annotate(f"Min: {min_data['spread']:.2f}\n{min_data['trade_date']}\n({min_data['spread_label']})",
                   xy=(min_date, min_data['spread']),
                   xytext=(15, -30), textcoords='offset points',
                   fontsize=9, color='blue',
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='blue', alpha=0.8),
                   arrowprops=dict(arrowstyle='->', color='blue'))

    # Add zero line
    ax.axhline(y=0, color='black', linestyle='--', linewidth=0.8, alpha=0.5)

    ax.set_ylabel('Spread (EUR/MWh)', fontsize=11)
    ax.set_xlabel('Trade Date', fontsize=11)
    ax.set_title(f'{contract_name} - Q1-Q2 Spread (Same Year) Historical Prices', fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='upper right')

    plt.tight_layout()

    # Save the figure
    output_file = f'{contract_name.lower().replace(" ", "_")}_q1_q2_spread_historical.png'
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"\nChart saved to: {output_file}")
    plt.close()


def export_to_excel(spread_df: pd.DataFrame, contract: str, contract_name: str):
    """Export detailed spread data to Excel."""
    export_df = spread_df.copy()

    # Add detailed columns
    export_df['instrument'] = contract_name

    # Calculate contract start and end dates
    export_df['q1_start'] = export_df['contract_year'].apply(lambda y: date(y, 1, 1))
    export_df['q1_end'] = export_df['contract_year'].apply(lambda y: date(y, 3, 31))
    export_df['q2_start'] = export_df['contract_year'].apply(lambda y: date(y, 4, 1))
    export_df['q2_end'] = export_df['contract_year'].apply(lambda y: date(y, 6, 30))

    # Rename and reorder columns
    export_df = export_df.rename(columns={
        'trade_date': 'historical_date'
    })

    # Select and order final columns
    export_df = export_df[[
        'instrument',
        'historical_date',
        'contract_year',
        'q1_start',
        'q1_end',
        'q1_price',
        'q2_start',
        'q2_end',
        'q2_price',
        'spread',
        'spread_label'
    ]]

    # Sort by date
    export_df = export_df.sort_values('historical_date')

    # Export to Excel
    output_file = f'{contract_name.lower().replace(" ", "_")}_q1_q2_spread_details.xlsx'
    export_df.to_excel(output_file, index=False, sheet_name='Q1-Q2 Spread')
    print(f"Excel file saved to: {output_file}")

    return export_df


def main():
    print(f"\n{'='*60}")
    print(f"Historical Q1-Q2 Spread Analysis")
    print(f"Contract: {CONTRACT} ({CONTRACT_NAMES.get(CONTRACT, CONTRACT)})")
    print(f"{'='*60}\n")

    # Load data (Trino first, PostgreSQL fallback)
    print(f"Loading settlement data for {CONTRACT}...")
    df = load_settlement_data(CONTRACT)

    if df.empty:
        print(f"No data found for contract {CONTRACT}")
        return

    hub = df['hub'].iloc[0]
    print(f"Hub: {hub}")
    print(f"Trade dates: {df['trade_date'].min()} to {df['trade_date'].max()}")
    print(f"Total records: {len(df):,}")

    # Calculate Q1-Q2 spread
    print("\nCalculating Q1-Q2 spread (same year, next calendar year)...")
    spread_df = calculate_q1_q2_spread(df)

    spread_count = len(spread_df)
    print(f"Valid Q1-Q2 spreads calculated: {spread_count:,}")

    if spread_df.empty:
        print("No valid spread data found.")
        return

    # Find extremes
    print("\nFinding historical extremes...")
    extremes = find_historical_extremes(spread_df)

    # Display results
    print(f"\n{'='*60}")
    print("RESULTS - Historical Q1-Q2 Spread Extremes")
    print(f"{'='*60}\n")

    if 'max' in extremes:
        data = extremes['max']
        print(f"Maximum Spread (Backwardation):")
        print(f"  Spread:        {data['spread']:.3f}")
        print(f"  Q1 Price:      {data['q1_price']:.3f}")
        print(f"  Q2 Price:      {data['q2_price']:.3f}")
        print(f"  Trade Date:    {data['trade_date']}")
        print(f"  Contract:      {data['spread_label']}")
        print()

    if 'min' in extremes:
        data = extremes['min']
        print(f"Minimum Spread (Contango):")
        print(f"  Spread:        {data['spread']:.3f}")
        print(f"  Q1 Price:      {data['q1_price']:.3f}")
        print(f"  Q2 Price:      {data['q2_price']:.3f}")
        print(f"  Trade Date:    {data['trade_date']}")
        print(f"  Contract:      {data['spread_label']}")
        print()

    # Show some context - top 10 widest spreads (both directions)
    print(f"\n{'='*60}")
    print("Top 10 Widest Backwardation (Q1 > Q2)")
    print(f"{'='*60}\n")
    top_backwardation = spread_df.nlargest(10, 'spread')[['trade_date', 'spread_label', 'q1_price', 'q2_price', 'spread']]
    print(top_backwardation.to_string(index=False))

    print(f"\n{'='*60}")
    print("Top 10 Widest Contango (Q1 < Q2)")
    print(f"{'='*60}\n")
    top_contango = spread_df.nsmallest(10, 'spread')[['trade_date', 'spread_label', 'q1_price', 'q2_price', 'spread']]
    print(top_contango.to_string(index=False))

    # Generate chart
    contract_name = CONTRACT_NAMES.get(CONTRACT, CONTRACT)
    plot_spread(spread_df, extremes, contract_name)

    # Export to Excel
    print(f"\n{'='*60}")
    print("Exporting to Excel")
    print(f"{'='*60}")
    export_to_excel(spread_df, CONTRACT, contract_name)


if __name__ == '__main__':
    main()
