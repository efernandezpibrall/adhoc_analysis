"""February LNG Supply: Middle East vs Rest of World - Stacked Bar Chart"""

import configparser
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine

config = configparser.ConfigParser(interpolation=None)
config.read('/home/efernandez/development/Github/config.ini')
engine = create_engine(config['DATABASE']['CONNECTION_STRING'])

df = pd.read_sql("""
    SELECT w.year,
           CASE WHEN w.country_name IN ('Qatar', 'United Arab Emirates') THEN 'Qatar + UAE'
                ELSE 'Rest of World' END as region,
           ROUND(SUM(w.metric_value)::numeric, 1) as total_supply
    FROM at_lng.woodmac_lng_plant_train_monthly_output_mta w
    LEFT JOIN at_lng.mappings_country mc ON w.country_name = mc.country
    WHERE w.month = 2
    GROUP BY w.year, region
    ORDER BY w.year, region
""", engine)
engine.dispose()

df['label'] = 'Feb-' + df['year'].astype(str)

REGIONS = [
    ('Rest of World', '#FF7043'),
    ('Qatar + UAE', '#00ACC1'),
]

fig = go.Figure()
for region, color in REGIONS:
    rdf = df[df['region'] == region]
    if not rdf.empty:
        fig.add_trace(go.Bar(
            x=rdf['label'], y=rdf['total_supply'], name=region,
            marker_color=color, text=rdf['total_supply'], textposition='inside',
            texttemplate='%{text:.1f}'
        ))

fig.update_layout(
    title='LNG Supply in February - Qatar + UAE vs Rest of World (mmtpa)',
    xaxis_title='', yaxis_title='Total Supply (mmtpa)',
    barmode='stack', template='plotly_white',
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
    yaxis=dict(dtick=20)
)
output_path = '/home/efernandez/development/Github/adhoc_analysis/feb_supply_me_vs_row.html'
fig.write_html(output_path)
print(f"Chart saved to {output_path}")
