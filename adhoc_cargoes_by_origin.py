"""
Adhoc: LNG Cargoes by Origin Country to Europe (Last 24 Months + 2-Month Forecast)
Stacked bar chart showing volumes in mcm/d per origin country.
"""

import calendar
import configparser
from sqlalchemy import create_engine
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Database connection
config = configparser.ConfigParser(interpolation=None)
config.read('/home/efernandez/development/Github/config.ini')
engine = create_engine(config['DATABASE']['CONNECTION_STRING'], pool_pre_ping=True)

query = """
    SELECT origin_country_name,
           DATE_TRUNC('month', "end")::date as month,
           ROUND((SUM(cargo_destination_cubic_meters) * 0.0006)::numeric, 2) as volume_mcm,
           CASE WHEN status = 'Delivered' THEN 'Historical' ELSE 'Forecast' END as data_type
    FROM at_lng.kpler_trades
    WHERE upload_timestamp_utc = (SELECT MAX(upload_timestamp_utc) FROM at_lng.kpler_trades)
      AND destination_country_name IN ('France', 'United Kingdom', 'Belgium', 'Italy', 'Netherlands', 'Germany')
      AND "end" >= NOW() - INTERVAL '24 months'
      AND "end" <= NOW() + INTERVAL '2 months'
    GROUP BY origin_country_name, month, data_type
    ORDER BY month
"""

df = pd.read_sql(query, engine)
engine.dispose()

# Combine historical + forecast per origin/month (sum volumes)
df_agg = df.groupby(['origin_country_name', 'month']).agg(
    volume_mcm=('volume_mcm', 'sum'),
    data_type=('data_type', 'first')
).reset_index()

# Convert mcm to mcm/d (divide by days in month)
df_agg['days_in_month'] = pd.to_datetime(df_agg['month']).apply(lambda x: calendar.monthrange(x.year, x.month)[1])
df_agg['volume_mcm'] = (df_agg['volume_mcm'] / df_agg['days_in_month']).round(2)

# Determine forecast months
forecast_months = set(df.loc[df['data_type'] == 'Forecast', 'month'])

# Format x-axis labels as MMM-YY (shorter)
df_agg['month_label'] = pd.to_datetime(df_agg['month']).dt.strftime('%b-%y')
all_months = df_agg.sort_values('month')['month'].unique()
month_labels = [pd.Timestamp(m).strftime('%b-%y') for m in all_months]

# Get top origins by total volume, group the rest as "Other"
top_n = 10
origin_totals = df_agg.groupby('origin_country_name')['volume_mcm'].sum().sort_values(ascending=False)
top_origins = origin_totals.head(top_n).index.tolist()
df_agg.loc[~df_agg['origin_country_name'].isin(top_origins), 'origin_country_name'] = 'Other'
df_agg = df_agg.groupby(['origin_country_name', 'month', 'month_label']).agg(
    volume_mcm=('volume_mcm', 'sum')
).reset_index()

# Build pivot table (origins as rows, months as columns)
pivot = df_agg.pivot_table(index='origin_country_name', columns='month_label', values='volume_mcm', aggfunc='sum', fill_value=0)
pivot = pivot[month_labels]
pivot.loc['Total'] = pivot.sum()
pivot = pivot.round(2)

row_order = pivot.drop('Total').sum(axis=1).sort_values(ascending=False).index.tolist() + ['Total']
pivot = pivot.loc[row_order]

# Mark forecast columns
fcst_labels = [pd.Timestamp(m).strftime('%b-%y') for m in forecast_months]
header_labels = [f'{c} *' if c in fcst_labels else c for c in pivot.columns]

# ── Build percentage pivot ──
pivot_no_total = pivot.drop('Total')
pct_pivot = pivot_no_total.div(pivot_no_total.sum()).mul(100).round(1)
pct_pivot.loc['Total'] = pct_pivot.sum().round(1)
pct_pivot = pct_pivot.loc[pivot.index]  # keep same row order

# ── Build figure (4 rows: vol chart, vol table, pct chart, pct table) ──
fig = make_subplots(
    rows=4, cols=1,
    row_heights=[0.35, 0.20, 0.25, 0.20],
    specs=[[{"type": "bar"}], [{"type": "table"}], [{"type": "bar"}], [{"type": "table"}]],
    vertical_spacing=0.04,
)

origins_sorted = df_agg.groupby('origin_country_name')['volume_mcm'].sum().sort_values(ascending=False).index

# Distinct color palette
colors = px.colors.qualitative.Bold + px.colors.qualitative.Vivid
color_map = {origin: colors[i % len(colors)] for i, origin in enumerate(origins_sorted)}

for origin in origins_sorted:
    origin_df = df_agg[df_agg['origin_country_name'] == origin].sort_values('month')
    color = color_map[origin]

    hist_mask = ~origin_df['month'].isin(forecast_months)
    fcst_mask = origin_df['month'].isin(forecast_months)

    if hist_mask.any():
        hist_df = origin_df[hist_mask]
        fig.add_trace(go.Bar(
            x=hist_df['month_label'],
            y=hist_df['volume_mcm'],
            name=origin,
            legendgroup=origin,
            marker_color=color,
        ), row=1, col=1)

    if fcst_mask.any():
        fcst_df = origin_df[fcst_mask]
        fig.add_trace(go.Bar(
            x=fcst_df['month_label'],
            y=fcst_df['volume_mcm'],
            name=f'{origin} (Fcst)',
            legendgroup=origin,
            showlegend=False,
            marker_color=color,
            marker_pattern_shape='/',
        ), row=1, col=1)

# ── Volume Table (transposed: months as rows, origins as columns) ──
pivot_t = pivot.T  # columns = origins, index = month labels
pivot_t.loc['Total'] = pivot_t.sum()
n_rows_t = len(pivot_t)

# Row colors: forecast months highlighted, alternating otherwise, Total bold
month_row_colors = []
for i, ml in enumerate(pivot_t.index):
    if ml == 'Total':
        month_row_colors.append('#e0e0e0')
    elif ml in fcst_labels:
        month_row_colors.append('#dce9f7')
    else:
        month_row_colors.append('#f9f9f9' if i % 2 == 0 else 'white')

# Format month labels (mark forecast)
month_vals = [f'<b>{ml} *</b>' if ml in fcst_labels else (f'<b>{ml}</b>' if ml == 'Total' else ml) for ml in pivot_t.index]

# Format number columns
vol_num_cols = []
for origin in pivot_t.columns:
    col_vals = []
    for i, v in enumerate(pivot_t[origin].tolist()):
        formatted = f'{v:.2f}'
        col_vals.append(f'<b>{formatted}</b>' if pivot_t.index[i] == 'Total' else formatted)
    vol_num_cols.append(col_vals)

vol_col_widths = [80] + [60] * len(pivot_t.columns)

fig.add_trace(go.Table(
    columnwidth=vol_col_widths,
    header=dict(
        values=['<b>Month</b>'] + [f'<b>{o}</b>' for o in pivot_t.columns],
        fill_color='#2c3e50',
        font=dict(color='white', size=10, family='Arial'),
        align='center',
        height=26,
    ),
    cells=dict(
        values=[month_vals] + vol_num_cols,
        fill_color=[month_row_colors] * (len(pivot_t.columns) + 1),
        font=dict(size=10, family='Arial'),
        align=['left'] + ['center'] * len(pivot_t.columns),
        height=22,
    ),
), row=2, col=1)

# ── Percentage chart (100% stacked bar) ──
# Compute percentage per month for chart data
month_totals = df_agg.groupby('month_label')['volume_mcm'].transform('sum')
df_agg['pct'] = (df_agg['volume_mcm'] / month_totals * 100).round(1)

for origin in origins_sorted:
    origin_df = df_agg[df_agg['origin_country_name'] == origin].sort_values('month')
    color = color_map[origin]

    hist_mask = ~origin_df['month'].isin(forecast_months)
    fcst_mask = origin_df['month'].isin(forecast_months)

    if hist_mask.any():
        hist_df = origin_df[hist_mask]
        fig.add_trace(go.Bar(
            x=hist_df['month_label'],
            y=hist_df['pct'],
            name=origin,
            legendgroup=origin,
            showlegend=False,
            marker_color=color,
            hovertemplate='%{y:.1f}%<extra>' + origin + '</extra>',
        ), row=3, col=1)

    if fcst_mask.any():
        fcst_df = origin_df[fcst_mask]
        fig.add_trace(go.Bar(
            x=fcst_df['month_label'],
            y=fcst_df['pct'],
            name=f'{origin} (Fcst)',
            legendgroup=origin,
            showlegend=False,
            marker_color=color,
            marker_pattern_shape='/',
            hovertemplate='%{y:.1f}%<extra>' + origin + '</extra>',
        ), row=3, col=1)

# ── Percentage table (transposed: months as rows, origins as columns) ──
pct_pivot_t = pct_pivot.T
pct_pivot_t.loc['Total'] = pct_pivot_t.sum().round(1)
pct_n_rows_t = len(pct_pivot_t)

pct_month_row_colors = []
for i, ml in enumerate(pct_pivot_t.index):
    if ml == 'Total':
        pct_month_row_colors.append('#e0e0e0')
    elif ml in fcst_labels:
        pct_month_row_colors.append('#dce9f7')
    else:
        pct_month_row_colors.append('#f9f9f9' if i % 2 == 0 else 'white')

pct_month_vals = [f'<b>{ml} *</b>' if ml in fcst_labels else (f'<b>{ml}</b>' if ml == 'Total' else ml) for ml in pct_pivot_t.index]

pct_num_cols = []
for origin in pct_pivot_t.columns:
    col_vals = []
    for i, v in enumerate(pct_pivot_t[origin].tolist()):
        formatted = f'{v:.1f}%'
        col_vals.append(f'<b>{formatted}</b>' if pct_pivot_t.index[i] == 'Total' else formatted)
    pct_num_cols.append(col_vals)

fig.add_trace(go.Table(
    columnwidth=vol_col_widths,
    header=dict(
        values=['<b>Month</b>'] + [f'<b>{o}</b>' for o in pct_pivot_t.columns],
        fill_color='#2c3e50',
        font=dict(color='white', size=10, family='Arial'),
        align='center',
        height=26,
    ),
    cells=dict(
        values=[pct_month_vals] + pct_num_cols,
        fill_color=[pct_month_row_colors] * (len(pct_pivot_t.columns) + 1),
        font=dict(size=10, family='Arial'),
        align=['left'] + ['center'] * len(pct_pivot_t.columns),
        height=22,
    ),
), row=4, col=1)

# ── Layout ──
fig.update_layout(
    barmode='stack',
    title=dict(
        text='LNG Cargoes by Origin Country to Europe (mcm/d)',
        font=dict(size=16),
    ),
    yaxis_title='mcm/d',
    xaxis=dict(
        categoryorder='array',
        categoryarray=month_labels,
        tickangle=-45,
        tickfont=dict(size=10),
    ),
    template='plotly_white',
    height=1600,
    margin=dict(l=50, r=30, t=80, b=10),
    legend=dict(
        orientation='h',
        yanchor='bottom',
        y=1.02,
        xanchor='center',
        x=0.5,
        font=dict(size=10),
    ),
    hovermode='x unified',
    # Percentage chart axes
    xaxis3=dict(
        categoryorder='array',
        categoryarray=month_labels,
        tickangle=-45,
        tickfont=dict(size=10),
    ),
    yaxis3=dict(
        title='%',
        range=[0, 100],
    ),
)

fig.update_xaxes(categoryorder='array', categoryarray=month_labels, row=3, col=1)

# Add annotations
fig.add_annotation(
    text='* Forecast (in-transit cargoes)',
    xref='paper', yref='paper',
    x=1, y=0.57,
    showarrow=False,
    font=dict(size=10, color='grey', style='italic'),
    xanchor='right',
)
fig.add_annotation(
    text='<b>Percentage Contribution by Origin (%)</b>',
    xref='paper', yref='paper',
    x=0, y=0.44,
    showarrow=False,
    font=dict(size=14),
    xanchor='left',
)

output_path = '/home/efernandez/development/Github/adhoc_analysis/adhoc_cargoes_by_origin.html'
fig.write_html(output_path)
print(f'Chart saved to {output_path}')
