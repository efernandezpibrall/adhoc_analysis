#!/usr/bin/env python3
"""
Fast IGA Trading Table Generator

Generates detailed trading table for IGA_202601-IGA_Q12026 pair showing:
- All 6 trades with entry/exit signals
- Daily mark-to-market returns
- Portfolio evolution and drawdown calculations

Uses standalone simulation module (< 1 second execution vs 8+ minutes)
"""

import pandas as pd
import numpy as np
import configparser
from sqlalchemy import create_engine, text
import sys
import os

# Add project root to path for imports
sys.path.insert(0, '/home/efernandez/development/Github')

# Import standalone simulation functions (instant import, no 8-minute pipeline!)
from strategies.pairs_trading.simulation_standalone import (
    simulate_historical_trading_returns,
    convert_to_calendar_time_returns
)

print("="*150)
print("GENERATING COMPLETE TRADING TABLE: IGA_202601-IGA_Q12026 (90-day window)")
print("="*150)

# ==================== STEP 1: FETCH DATA ====================
print("\n[1/5] Fetching data from database...")

config = configparser.ConfigParser()
config.read('/home/efernandez/development/Github/config.ini')
engine = create_engine(config['DATABASE']['CONNECTION_STRING'])

with engine.connect() as conn:
    # Get IGA Jan 2026
    query_jan = text("""
        SELECT trade_date, settlement_price
        FROM at_lng.cleared_gas
        WHERE contract = 'IGA' AND strip = '2026-01-01'
        ORDER BY trade_date
    """)
    df_jan = pd.read_sql(query_jan, conn)
    df_jan.columns = ['trade_date', 'price_jan']

    # Get IGA Q1 2026 (average of Jan, Feb, Mar)
    query_q1 = text("""
        SELECT trade_date, AVG(settlement_price) as settlement_price
        FROM at_lng.cleared_gas
        WHERE contract = 'IGA'
        AND strip IN ('2026-01-01', '2026-02-01', '2026-03-01')
        GROUP BY trade_date
        ORDER BY trade_date
    """)
    df_q1 = pd.read_sql(query_q1, conn)
    df_q1.columns = ['trade_date', 'price_q1']

    # Get expected database results for validation
    query_db = text("""
        SELECT
            num_simulated_trades,
            max_drawdown_pct,
            profit_factor,
            sharpe_ratio,
            sortino_ratio
        FROM at_lng.pairs_strategy_stats
        WHERE pair = 'IGA_202601-IGA_Q12026' AND n_days = 90
    """)
    db_results = pd.read_sql(query_db, conn).iloc[0]

print(f"✓ Fetched {len(df_jan)} days for IGA_202601")
print(f"✓ Fetched {len(df_q1)} days for IGA_Q12026")
print(f"\n  Database expects:")
print(f"    - Trades: {db_results['num_simulated_trades']}")
print(f"    - MaxDD: {db_results['max_drawdown_pct']:.6f}")
print(f"    - Profit Factor: {db_results['profit_factor']:.2f}")

# ==================== STEP 2: PREPARE DATA ====================
print("\n[2/5] Preparing data...")

# Merge and calculate spread
df = pd.merge(df_jan, df_q1, on='trade_date', how='inner')
df['spread'] = df['price_jan'] - df['price_q1']
df = df.sort_values('trade_date').reset_index(drop=True)

# Apply 90-day window
max_date = df['trade_date'].max()
cutoff_date = max_date - pd.Timedelta(days=90)
df_90 = df[df['trade_date'] > cutoff_date].copy().reset_index(drop=True)

print(f"✓ 90-day window: {df_90['trade_date'].min()} to {df_90['trade_date'].max()}")
print(f"✓ Total days: {len(df_90)}")

# Calculate spread statistics
spread_series = pd.Series(df_90['spread'].values)
spread_mean = spread_series.mean()
spread_std = spread_series.std()

print(f"✓ Spread stats: mean={spread_mean:.4f}, std={spread_std:.4f}")
print(f"  Entry thresholds: +2σ={spread_mean + 2*spread_std:.4f}, -2σ={spread_mean - 2*spread_std:.4f}")
print(f"  Exit threshold: ±0.5σ={spread_mean - 0.5*spread_std:.4f} to {spread_mean + 0.5*spread_std:.4f}")

# ==================== STEP 3: SIMULATE TRADES ====================
print("\n[3/5] Running simulation with EXACT strategy parameters...")

# Use EXACT same parameters as main strategy (from pairs_trading.py line 1478-1485)
trade_data = simulate_historical_trading_returns(
    spread_series=spread_series,
    spread_mean=spread_mean,
    spread_std=spread_std,
    entry_threshold=2.0,      # Entry at ±2σ (NOT 1σ!)
    exit_threshold=0.5,       # Exit at 0.5σ (NOT 0σ!)
    transaction_cost_pct=0.0015,  # 0.15% round-trip
    stop_loss_zscore=3.0,
    max_holding_days=40,
    stop_loss_drawdown_pct=0.15
)

num_trades = len(trade_data['returns'])
print(f"✓ Simulation complete: {num_trades} trades found")
print(f"  Entry indices: {trade_data['entry_indices']}")
print(f"  Exit indices: {trade_data['exit_indices']}")
print(f"  Exit reasons: {trade_data['exit_reasons']}")

# Validate trade count
if num_trades != db_results['num_simulated_trades']:
    print(f"\n⚠️  WARNING: Trade count mismatch!")
    print(f"  Expected: {db_results['num_simulated_trades']} trades")
    print(f"  Got: {num_trades} trades")
else:
    print(f"✓ Trade count matches database: {num_trades} trades")

# ==================== STEP 4: CALCULATE MTM RETURNS ====================
print("\n[4/5] Calculating mark-to-market daily returns...")

# Convert to calendar-time returns with MTM
daily_returns = convert_to_calendar_time_returns(
    trade_data=trade_data,
    num_days=len(spread_series),
    spread_series=spread_series
)

print(f"✓ Daily returns calculated")
print(f"  Non-zero return days: {np.count_nonzero(daily_returns)}")

# Calculate portfolio evolution
cumulative_returns = np.cumprod(1 + daily_returns)
running_max = np.maximum.accumulate(cumulative_returns)
drawdown_abs = running_max - cumulative_returns
drawdown_pct = ((cumulative_returns / running_max) - 1) * 100

max_dd_abs = drawdown_abs.max()
max_dd_pct = drawdown_pct.min()

print(f"\n  Portfolio evolution:")
print(f"    Starting: ${cumulative_returns[0]:.4f}")
print(f"    Ending: ${cumulative_returns[-1]:.4f}")
print(f"    Peak: ${running_max.max():.4f}")
print(f"    MaxDD (absolute): {max_dd_abs:.6f}")
print(f"    MaxDD (percentage): {max_dd_pct:.2f}%")

# Validate MaxDD
if abs(max_dd_abs - db_results['max_drawdown_pct']) < 0.001:
    print(f"✓ MaxDD matches database: {max_dd_abs:.6f}")
else:
    print(f"\n⚠️  WARNING: MaxDD mismatch!")
    print(f"  Expected: {db_results['max_drawdown_pct']:.6f}")
    print(f"  Got: {max_dd_abs:.6f}")

# Calculate z-scores
df_90['z_score'] = (df_90['spread'] - spread_mean) / spread_std

# ==================== STEP 5: BUILD TRADING TABLE ====================
print("\n[5/5] Building detailed trading table...")

# Add portfolio columns
df_90['trade_num'] = 0
df_90['trade_signal'] = ''
df_90['signal_reason'] = ''
df_90['position'] = 0
df_90['daily_return'] = daily_returns
df_90['portfolio_value'] = cumulative_returns
df_90['peak_value'] = running_max
df_90['drawdown_abs'] = drawdown_abs
df_90['drawdown_pct'] = drawdown_pct

# Mark entries and exits
for i, (entry_idx, exit_idx, ret, pos, reason) in enumerate(zip(
    trade_data['entry_indices'],
    trade_data['exit_indices'],
    trade_data['returns'],
    trade_data['positions'],
    trade_data['exit_reasons']
)):
    trade_num = i + 1

    # Mark entry
    df_90.at[entry_idx, 'trade_num'] = trade_num
    df_90.at[entry_idx, 'trade_signal'] = f'ENTRY #{trade_num}'
    z = df_90.at[entry_idx, 'z_score']
    if pos > 0:
        df_90.at[entry_idx, 'signal_reason'] = f'Spread below -2σ (z={z:.2f}) → LONG spread (buy Jan, sell Q1)'
    else:
        df_90.at[entry_idx, 'signal_reason'] = f'Spread above +2σ (z={z:.2f}) → SHORT spread (sell Jan, buy Q1)'
    df_90.at[entry_idx, 'position'] = pos

    # Mark exit
    df_90.at[exit_idx, 'trade_num'] = trade_num
    df_90.at[exit_idx, 'trade_signal'] = f'EXIT #{trade_num}'
    z = df_90.at[exit_idx, 'z_score']
    df_90.at[exit_idx, 'signal_reason'] = f'{reason} (z={z:.2f}) | Return: {ret*100:.2f}%'

    # Mark position for all days in between
    for day in range(entry_idx, exit_idx + 1):
        if day != entry_idx and day != exit_idx:
            df_90.at[day, 'position'] = pos
            df_90.at[day, 'trade_num'] = trade_num

# Save to CSV
output_file = '/home/efernandez/development/Github/IGA_trading_table_CORRECT.csv'
df_90.to_csv(output_file, index=False)

print(f"✓ Table saved to: {output_file}")

# ==================== DISPLAY TABLE ====================
print(f"\n{'='*150}")
print("COMPLETE TRADING TABLE")
print(f"{'='*150}\n")

# Print formatted table
print(f"{'Date':<12} {'Jan':<9} {'Q1':<9} {'Spread':<9} {'Z':<7} {'Signal':<13} {'Pos':<6} {'Reason':<50} {'Portfolio':<11} {'DD(abs)':<9} {'DD(%)':<8}")
print("-" * 150)

for idx, row in df_90.iterrows():
    date_str = str(row['trade_date'])[:10]
    jan = f"{row['price_jan']:.3f}"
    q1 = f"{row['price_q1']:.3f}"
    spread = f"{row['spread']:.4f}"
    z = f"{row['z_score']:.2f}"
    signal = row['trade_signal'] if row['trade_signal'] else '-'
    pos_str = 'LONG' if row['position'] > 0 else ('SHORT' if row['position'] < 0 else '-')
    reason = row['signal_reason'][:48] if row['signal_reason'] else '-'
    portfolio = f"${row['portfolio_value']:.4f}"
    dd_abs = f"{row['drawdown_abs']:.4f}"
    dd_pct = f"{row['drawdown_pct']:.2f}%"

    marker = ""
    if 'ENTRY' in signal:
        marker = " ◄◄◄ ENTRY"
        print("-" * 150)
    elif 'EXIT' in signal:
        marker = " ◄◄◄ EXIT"
    elif row['drawdown_abs'] == df_90['drawdown_abs'].max():
        marker = " ◄◄◄ MAX DD!"

    print(f"{date_str:<12} {jan:<9} {q1:<9} {spread:<9} {z:<7} {signal:<13} {pos_str:<6} {reason:<50} {portfolio:<11} {dd_abs:<9} {dd_pct:<8}{marker}")

    if 'EXIT' in signal:
        print("-" * 150)

# Print trade summary
print(f"\n{'='*150}")
print("TRADE SUMMARY")
print(f"{'='*150}\n")

for i, (entry_idx, exit_idx, ret, pos, reason) in enumerate(zip(
    trade_data['entry_indices'],
    trade_data['exit_indices'],
    trade_data['returns'],
    trade_data['positions'],
    trade_data['exit_reasons']
)):
    trade_num = i + 1
    entry_date = df_90.loc[entry_idx, 'trade_date']
    exit_date = df_90.loc[exit_idx, 'trade_date']
    entry_spread = df_90.loc[entry_idx, 'spread']
    exit_spread = df_90.loc[exit_idx, 'spread']
    entry_z = df_90.loc[entry_idx, 'z_score']
    exit_z = df_90.loc[exit_idx, 'z_score']
    holding_days = exit_idx - entry_idx

    print(f"Trade {trade_num}:")
    print(f"  Entry: {entry_date} | Spread={entry_spread:.4f} | Z={entry_z:.2f} | Position={'LONG' if pos > 0 else 'SHORT'}")
    print(f"  Exit:  {exit_date} | Spread={exit_spread:.4f} | Z={exit_z:.2f} | Reason={reason}")
    print(f"  Holding: {holding_days} days | Return: {ret:.4f} ({ret*100:.2f}%)\n")

# Final validation
print(f"{'='*150}")
print("VALIDATION SUMMARY")
print(f"{'='*150}\n")

wins = len([r for r in trade_data['returns'] if r > 0])
losses = len([r for r in trade_data['returns'] if r < 0])
win_rate = wins / num_trades * 100 if num_trades > 0 else 0

print(f"✓ Number of Trades: {num_trades} (Expected: {db_results['num_simulated_trades']})")
print(f"✓ Win Rate: {win_rate:.1f}%")
print(f"✓ MaxDD (absolute): {max_dd_abs:.6f} (Expected: {db_results['max_drawdown_pct']:.6f})")
print(f"✓ Final Portfolio: ${cumulative_returns[-1]:.4f}")
print(f"✓ Total Return: {(cumulative_returns[-1] - 1)*100:.2f}%")

max_dd_idx = df_90['drawdown_abs'].idxmax()
print(f"\n✓ MaxDD occurred on: {df_90.loc[max_dd_idx, 'trade_date']}")
print(f"  At portfolio value: ${df_90.loc[max_dd_idx, 'portfolio_value']:.4f}")
print(f"  Peak was: ${df_90.loc[max_dd_idx, 'peak_value']:.4f}")

print(f"\n{'='*150}")
print(f"✓ COMPLETE! Table saved to: {output_file}")
print(f"{'='*150}")
