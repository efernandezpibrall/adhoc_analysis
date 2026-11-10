import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np
from matplotlib.patches import Rectangle
import matplotlib.patches as mpatches
import configparser
from sqlalchemy import create_engine

from pathlib import Path

# Set the style for professional appearance
plt.style.use('seaborn-v0_8-whitegrid')

# Set font to DejaVu Sans (widely available on Linux)
plt.rcParams['font.family'] = 'DejaVu Sans'

# Load database configuration
CONFIG_FILE_PATH = Path(__file__).resolve().parents[1] / "config.ini"

config_reader = configparser.ConfigParser(interpolation=None)
config_reader.read(CONFIG_FILE_PATH)

# Database configuration
db_connection_string = config_reader.get('DATABASE', 'CONNECTION_STRING')
db_schema = config_reader.get('DATABASE', 'SCHEMA')

# Create database engine
engine = create_engine(db_connection_string)

# SQL query to get data with internal adjustments taking priority over Woodmac
query = f"""
WITH capacity_max AS (
    SELECT
        id_plant,
        id_lng_train,
        MAX(metric_value) as max_capacity
    FROM {db_schema}.woodmac_lng_plant_monthly_capacity_nominal_mta c
    GROUP BY id_plant, id_lng_train
),
woodmac_baseline AS (
    SELECT
        b.plant_name,
        b.country_name,
        a.lng_train_date_start_est,
        cm.max_capacity as capacity,
        a.id_plant,
        a.id_lng_train
    FROM {db_schema}.woodmac_lng_plant_train a
    JOIN {db_schema}.woodmac_lng_plant_summary b ON a.id_plant = b.id_plant
    JOIN capacity_max cm ON a.id_plant = cm.id_plant AND a.id_lng_train = cm.id_lng_train
    WHERE a.lng_train_date_start_est IS NOT NULL
),
internal_adjustments AS (
    SELECT DISTINCT ON (id_plant, id_lng_train)
        plant_name,
        lng_train_date_start_est,
        COALESCE(nameplate_capacity, 0) as capacity,
        id_plant,
        id_lng_train
    FROM {db_schema}.fundamentals_terminals_trains
    WHERE source_name = 'internal'
      AND scenario_name = 'base_case'
      AND lng_train_date_start_est IS NOT NULL
    ORDER BY id_plant, id_lng_train, upload_timestamp_utc DESC
)
SELECT
    COALESCE(ia.plant_name, wb.plant_name) as plant_name,
    wb.country_name,
    COALESCE(ia.lng_train_date_start_est::date, wb.lng_train_date_start_est::date) as lng_train_date_start_est,
    COALESCE(ia.capacity, wb.capacity) as capacity,
    wb.lng_train_date_start_est::date as woodmac_date,
    ia.lng_train_date_start_est::date as internal_date,
    CASE WHEN ia.lng_train_date_start_est IS NOT NULL THEN 'internal' ELSE 'woodmac' END as data_source
FROM woodmac_baseline wb
LEFT JOIN internal_adjustments ia ON wb.id_plant = ia.id_plant AND wb.id_lng_train = ia.id_lng_train
ORDER BY wb.country_name, COALESCE(ia.plant_name, wb.plant_name), COALESCE(ia.lng_train_date_start_est::date, wb.lng_train_date_start_est::date)
"""

# Execute query and read data
print("Fetching train data with internal adjustments prioritized over Woodmac baseline...")
df = pd.read_sql_query(query, engine)

# Convert start_date to datetime
df['lng_train_date_start_est'] = pd.to_datetime(df['lng_train_date_start_est'])

# Filter for trains starting in 2025 or later
df = df[df['lng_train_date_start_est'] >= '2025-01-01']

# Create version with capacity in Mcm/d (MTPA * 1.36 / 365 * 1000)
df_mcmd = df.copy()
df_mcmd['capacity'] = df_mcmd['capacity'] * 1.36 / 365 * 1000  # Convert MTPA to Mcm/d

# Create month-year column for aggregation (set to 1st of month)
df['month_start'] = df['lng_train_date_start_est'].dt.to_period('M').dt.to_timestamp()

# Sort by country and then by plant name
df = df.sort_values(['country_name', 'plant_name', 'month_start'])

# Aggregate by plant and month - sum capacity and count trains
monthly_df = df.groupby(['country_name', 'plant_name', 'month_start']).agg({
    'capacity': 'sum',
    'lng_train_date_start_est': 'count'  # Count of trains
}).reset_index()
monthly_df.rename(columns={'lng_train_date_start_est': 'train_count'}, inplace=True)

# Calculate adaptive scaling for better visual balance
max_capacity = monthly_df['capacity'].max()
print(f"Maximum capacity found: {max_capacity:.1f} MTPA")

# Smart scaling that maintains good visibility while preventing extreme overflow
def smart_scale_capacity(capacity):
    if capacity <= 8:
        return capacity  # Linear scaling for normal range
    elif capacity <= 20:
        # Gentle compression for medium-high range
        return 8 + (capacity - 8) * 0.7
    else:
        # More compression for very high values but still visible
        return 8 + 12 * 0.7 + (capacity - 20) * 0.4

monthly_df['visual_capacity'] = monthly_df['capacity'].apply(smart_scale_capacity)

# Get unique plants with their countries
plants_df = monthly_df.groupby(['country_name', 'plant_name']).first().reset_index()

# Professional color palette (McKinsey-style)
primary_colors = {
    'US (Gulf Coast)': '#003A6C',  # Navy Blue
    'Canada (British Columbia)': '#00A3E0',  # Light Blue
    'Mexico': '#6BCABA',  # Teal
    'Argentina': '#90B23C',  # Green
    'Mauritania': '#FFC72C',  # Yellow
    'Senegal': '#FFC72C',  # Yellow
    'Gabon': '#F58220',  # Orange
    'Congo': '#E03C31',  # Red
    'Nigeria': '#7F3F98',  # Purple
    'Qatar': '#A21F5A',  # Burgundy
    'Malaysia': '#005EB8',  # Royal Blue
    'Indonesia': '#00B5E2',  # Sky Blue
    'Australia': '#78BE20',  # Lime Green
    'United States': '#003A6C',  # Navy Blue
    'Canada': '#00A3E0',  # Light Blue
}

# Create figure with specific dimensions for presentation
fig, ax = plt.subplots(figsize=(16, 10))
fig.patch.set_facecolor('white')
ax.set_facecolor('white')

# Add title and subtitle
fig.suptitle('LNG Train Start Dates and Capacity (MTPA) | January 2025 - January 2028',
             fontsize=20, fontweight='bold', y=0.98)

# Plot timeline for each plant
y_position = 0
y_labels = []
y_ticks = []
country_sections = {}
current_country = None
country_start_y = 0

for idx, (_, plant_info) in enumerate(plants_df.iterrows()):
    country = plant_info['country_name']
    plant = plant_info['plant_name']

    # Track country sections
    if country != current_country:
        if current_country is not None:
            country_sections[current_country] = (country_start_y, y_position - 1)
        current_country = country
        country_start_y = y_position

    # Get all monthly aggregations for this plant
    plant_months = monthly_df[(monthly_df['country_name'] == country) & (monthly_df['plant_name'] == plant)]

    # Plot each month
    for _, month_data in plant_months.iterrows():
        # Get color for country
        color = primary_colors.get(country, '#666666')

        # Use both actual and visual capacity
        cumulative_capacity = month_data['capacity'] if pd.notna(month_data['capacity']) else 1.0
        visual_capacity = month_data['visual_capacity']
        train_count = month_data['train_count']

        # Better scaling for visual appeal - maintain the original chart's proportions
        base_scale = 0.12  # Similar to original 0.15 scale
        max_bar_height = 0.45  # Allow taller bars but prevent extreme overflow
        bar_height = min(visual_capacity * base_scale, max_bar_height)

        # Create gradient effect for high-capacity bars
        if cumulative_capacity > 15:
            # Use gradient for very high capacity bars
            bar_alpha = 0.9
            edge_color = color
            edge_width = 2
        else:
            bar_alpha = 0.85
            edge_color = 'none'
            edge_width = 0

        # Create vertical bar with optimal proportions
        bar_width = 10  # Wider for better visibility and text spacing
        bar = Rectangle((mdates.date2num(month_data['month_start']) - bar_width/2, y_position - bar_height/2),
                       bar_width,
                       bar_height,
                       facecolor=color,
                       edgecolor=edge_color,
                       linewidth=edge_width,
                       alpha=bar_alpha,
                       zorder=5)
        ax.add_patch(bar)

        # Add subtle white line at base for definition
        ax.plot([mdates.date2num(month_data['month_start']) - bar_width/2,
                mdates.date2num(month_data['month_start']) + bar_width/2],
               [y_position - bar_height/2, y_position - bar_height/2],
               color='white',
               linewidth=1.5,
               zorder=6)

        # Professional text formatting with clean design
        # Main capacity text above bar
        main_text = f"{cumulative_capacity:.1f}"

        # Train count text below bar (only for multiple trains)
        if train_count > 1:
            train_text = f"{train_count}"
        else:
            train_text = None

        # Always position text above the bar for consistency
        text_y = y_position + bar_height/2 + 0.15
        text_color = color
        text_size = 10  # Reduced by 1px as requested
        text_weight = 'bold'

        # Main capacity text - always prominent and readable
        ax.text(month_data['month_start'], text_y,
                main_text,
                fontsize=text_size,
                ha='center',
                va='center',
                color=text_color,
                weight=text_weight,
                zorder=8)

        # Train count label below bar (only for multiple trains)
        if train_text:
            train_label_y = y_position - bar_height/2 - 0.12  # Position below the bar
            ax.text(month_data['month_start'], train_label_y,
                    train_text,
                    fontsize=7,
                    ha='center',
                    va='center',
                    color=text_color,
                    weight='normal',
                    alpha=0.9,
                    zorder=8)


    y_labels.append(f"{plant}")
    y_ticks.append(y_position)

    # Add elegant horizontal line after each plant (except the last one)
    if idx < len(plants_df) - 1:
        ax.axhline(y=y_position + 0.5, color='#E0E0E0', linestyle='-', linewidth=0.5, alpha=0.7, zorder=2)

    y_position += 1

# Add the last country section
if current_country is not None:
    country_sections[current_country] = (country_start_y, y_position - 1)

# Add country section backgrounds - aligned with horizontal lines
for idx, (country, (start_y, end_y)) in enumerate(country_sections.items()):
    # The horizontal lines are at y + 0.5, so backgrounds should go from one line to the next
    if idx == 0:
        # First country: from top of chart to first horizontal line
        top_boundary = -0.5
    else:
        # Other countries: from previous horizontal line
        top_boundary = start_y - 0.5

    # Bottom boundary is at the horizontal line after the last plant of this country
    bottom_boundary = end_y + 0.5

    # Subtle background shading for country sections
    rect = Rectangle((mdates.date2num(datetime(2025, 1, 1)), top_boundary),
                    mdates.date2num(datetime(2028, 1, 31)) - mdates.date2num(datetime(2025, 1, 1)),
                    bottom_boundary - top_boundary,
                    facecolor=primary_colors.get(country, '#666666'),
                    alpha=0.03,  # Even more subtle
                    edgecolor='none',  # No edge to avoid lines
                    zorder=1)
    ax.add_patch(rect)

# Add arrows for internal adjustments showing change from Woodmac baseline
print("Adding arrows for internal adjustments...")
internal_trains = df[df['data_source'] == 'internal'].copy()

if len(internal_trains) > 0:
    # Create a mapping of plant to y_position
    plant_to_y = {}
    for i, plant in enumerate(monthly_df['plant_name'].unique()):
        # Find the y position for this plant
        for j, y_label in enumerate(y_labels):
            if plant in y_label:
                plant_to_y[plant] = y_ticks[j]
                break

    for _, train in internal_trains.iterrows():
        if pd.notna(train['woodmac_date']) and pd.notna(train['internal_date']):
            plant_name = train['plant_name']
            woodmac_date = pd.to_datetime(train['woodmac_date'])
            internal_date = pd.to_datetime(train['internal_date'])

            # Get y position for this plant
            y_pos = plant_to_y.get(plant_name)
            if y_pos is not None:
                # Only show arrow if dates are different and within our chart range
                if (woodmac_date != internal_date and
                    pd.Timestamp('2025-01-01') <= woodmac_date <= pd.Timestamp('2028-01-31') and
                    pd.Timestamp('2025-01-01') <= internal_date <= pd.Timestamp('2028-01-31')):

                    # Create arrow from woodmac_date to internal_date
                    arrow_y = y_pos - 0.3  # Slightly below the plant line

                    # Determine arrow color and style based on direction
                    if internal_date < woodmac_date:  # Moved earlier
                        arrow_color = '#2E8B57'  # Sea green for earlier
                        arrow_style = '->'
                        # Arrow FROM internal_date TO woodmac_date (pointing left)
                        arrow_start = mdates.date2num(woodmac_date)
                        arrow_end = mdates.date2num(internal_date)
                    else:  # Moved later
                        arrow_color = '#B22222'  # Fire brick for later
                        arrow_style = '->'
                        # Arrow FROM woodmac_date TO internal_date (pointing right)
                        arrow_start = mdates.date2num(woodmac_date)
                        arrow_end = mdates.date2num(internal_date)

                    # Add the arrow
                    ax.annotate('',
                               xy=(arrow_end, arrow_y),
                               xytext=(arrow_start, arrow_y),
                               arrowprops=dict(arrowstyle=arrow_style,
                                             color=arrow_color,
                                             lw=2,
                                             alpha=0.8,
                                             shrinkA=0, shrinkB=0),
                               zorder=9)

                    # Add small labels
                    mid_date = woodmac_date + (internal_date - woodmac_date) / 2
                    days_diff = abs((internal_date - woodmac_date).days)
                    months_diff = round(days_diff / 30.44, 1)  # Average days per month

                    # Label showing the change
                    label_text = f"{months_diff:.0f}m"

                    ax.text(mdates.date2num(mid_date), arrow_y + 0.15,
                           label_text,
                           fontsize=8,
                           ha='center',
                           va='bottom',
                           color=arrow_color,
                           weight='bold',
                           alpha=0.9,
                           zorder=10,
                           bbox=dict(boxstyle="round,pad=0.2",
                                   facecolor='white',
                                   edgecolor=arrow_color,
                                   alpha=0.8,
                                   linewidth=0.5))

print(f"Added {len(internal_trains)} internal adjustment arrows")

# Format x-axis - show every month
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%y'))
ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.xaxis.set_minor_locator(mdates.MonthLocator())

# Set x-axis limits with padding for country labels
ax.set_xlim(pd.Timestamp('2025-01-01'), pd.Timestamp('2028-01-31'))

# Remove axis labels
ax.set_xlabel('', fontsize=12, weight='bold')
ax.set_ylabel('', fontsize=12, weight='bold')

# Set y-axis
ax.set_yticks(y_ticks)
ax.set_yticklabels(y_labels, fontsize=10)
ax.set_ylim(-1, len(y_ticks))

# Create a mapping of y-position to country for coloring
y_to_country = {}
for country, (start_y, end_y) in country_sections.items():
    for i in range(int(start_y), int(end_y) + 1):
        y_to_country[i] = country

# Color y-axis labels according to their country
ytick_labels = ax.get_yticklabels()
for i, label in enumerate(ytick_labels):
    if i in y_to_country:
        country = y_to_country[i]
        color = primary_colors.get(country, '#666666')
        label.set_color(color)
        label.set_weight('bold')
        label.set_fontsize(11)

# Add vertical grid lines for each month
for year in [2025, 2026, 2027, 2028]:
    for month in range(1, 13):
        try:
            date = pd.Timestamp(f'{year}-{month:02d}-01')
            if pd.Timestamp('2025-01-01') <= date <= pd.Timestamp('2028-01-31'):
                # Major grid lines for January
                if month == 1:
                    ax.axvline(x=date, color='#666666', linestyle='-', linewidth=1.5, alpha=0.8, zorder=2)
                # Regular grid lines for other months
                else:
                    ax.axvline(x=date, color='#CCCCCC', linestyle='-', linewidth=0.8, alpha=0.6, zorder=2)
        except:
            break

# Remove horizontal grid
ax.grid(False, axis='y')

# Add current month marker
from datetime import datetime as dt
current_date = dt.now()
# Get first day of current month
first_of_month = current_date.replace(day=1)
if pd.Timestamp('2025-01-01') <= pd.Timestamp(first_of_month) <= pd.Timestamp('2028-01-31'):
    # Add vertical line for current month
    ax.axvline(x=first_of_month, color='#E74C3C', linestyle='--', linewidth=1.5, alpha=0.7, zorder=3)

    # Add "Today" label
    ax.text(first_of_month, ax.get_ylim()[1] - 0.5, 'Today',
            rotation=0, ha='center', va='bottom',
            fontsize=10, color='#E74C3C', weight='bold',
            bbox=dict(boxstyle="round,pad=0.3", facecolor='white', edgecolor='#E74C3C', alpha=0.8),
            zorder=10)

# Remove top and right spines for cleaner look
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_color('#CCCCCC')
ax.spines['bottom'].set_color('#CCCCCC')

# Adjust tick parameters
ax.tick_params(axis='both', which='major', labelsize=10, colors='#333333')
ax.tick_params(axis='y', which='major', length=0)  # Remove y-axis tick marks
ax.tick_params(axis='x', which='major', length=5)  # Keep x-axis tick marks

# Set x-axis label styling - make text larger and clearer
plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha='center', fontsize=9, weight='bold')

# Add country names on the right y-axis
ax2 = ax.twinx()  # Create a second y-axis
ax2.set_ylim(ax.get_ylim())  # Match the main y-axis limits

# Calculate middle position for each country and wrap long names
import textwrap

country_labels = []
country_positions = []
for country, (start_y, end_y) in country_sections.items():
    middle_y = (start_y + end_y) / 2
    country_positions.append(middle_y)

    # Wrap long country names
    if len(country) > 15:
        wrapped_country = '\n'.join(textwrap.wrap(country, width=15, break_long_words=False))
    else:
        wrapped_country = country
    country_labels.append(wrapped_country)

# Set country labels on right axis
ax2.set_yticks(country_positions)
ax2.set_yticklabels(country_labels, fontsize=12, fontweight='bold')

# Style the right axis
ax2.tick_params(axis='y', which='major', length=0)  # Remove tick marks
ax2.spines['right'].set_visible(False)
ax2.spines['top'].set_visible(False)
ax2.spines['bottom'].set_visible(False)
ax2.spines['left'].set_visible(False)
ax2.grid(False)  # Remove any grid lines from secondary axis

# Color the country labels
original_countries = list(country_sections.keys())
for i, (label, country) in enumerate(zip(ax2.get_yticklabels(), original_countries)):
    label.set_color(primary_colors.get(country, '#666666'))

# Add clean, professional note about chart elements
ax.text(0.02, 0.02, 'Bar height represents cumulative monthly capacity (MTPA) • Number below bar indicates multiple trains in same month • Arrows show internal adjustments vs Woodmac baseline',
        transform=ax.transAxes, fontsize=9,
        color='#666666', style='italic')


# Adjust layout
plt.tight_layout()
plt.subplots_adjust(bottom=0.08, right=0.88)  # Make room for country labels on right

# Save with high DPI for presentation quality
output_dir = Path(__file__).resolve().parent / "outputs" / "lng" / "terminals"
output_dir.mkdir(parents=True, exist_ok=True)
plt.savefig(output_dir / 'lng_timeline_postgres.png', dpi=300, bbox_inches='tight',
            facecolor='white', edgecolor='none')

print(f"MTPA Chart created successfully with {len(df)} prioritized train records aggregated into {len(monthly_df)} monthly entries")
print(f"File saved: {output_dir / 'lng_timeline_postgres.png'}")

# Close first chart to free memory
plt.close()

# Now create the same chart with Mcm/d units
print("\nCreating Mcm/d version...")

# Prepare Mcm/d data
df_mcmd['month_start'] = df_mcmd['lng_train_date_start_est'].dt.to_period('M').dt.to_timestamp()
df_mcmd = df_mcmd.sort_values(['country_name', 'plant_name', 'month_start'])

# Aggregate by plant and month - sum capacity and count trains
monthly_df_mcmd = df_mcmd.groupby(['country_name', 'plant_name', 'month_start']).agg({
    'capacity': 'sum',
    'lng_train_date_start_est': 'count'  # Count of trains
}).reset_index()
monthly_df_mcmd.rename(columns={'lng_train_date_start_est': 'train_count'}, inplace=True)

# Calculate adaptive scaling for better visual balance (adjusted for Mcm/d scale)
max_capacity_mcmd = monthly_df_mcmd['capacity'].max()
print(f"Maximum capacity found: {max_capacity_mcmd:.1f} Mcm/d")

# Smart scaling that maintains good visibility while preventing extreme overflow (adjusted for Mcm/d)
def smart_scale_capacity_mcmd(capacity):
    if capacity <= 30:
        return capacity  # Linear scaling for normal range
    elif capacity <= 75:
        # Gentle compression for medium-high range
        return 30 + (capacity - 30) * 0.7
    else:
        # More compression for very high values but still visible
        return 30 + 45 * 0.7 + (capacity - 75) * 0.4

monthly_df_mcmd['visual_capacity'] = monthly_df_mcmd['capacity'].apply(smart_scale_capacity_mcmd)

# Get unique plants with their countries
plants_df_mcmd = monthly_df_mcmd.groupby(['country_name', 'plant_name']).first().reset_index()

# Create figure with specific dimensions for presentation
fig2, ax2 = plt.subplots(figsize=(16, 10))
fig2.patch.set_facecolor('white')
ax2.set_facecolor('white')

# Add title and subtitle (updated for Mcm/d)
fig2.suptitle('LNG Train Start Dates and Capacity (Mcm/d) | January 2025 - January 2028',
             fontsize=20, fontweight='bold', y=0.98)

# Plot timeline for each plant (using same logic but with Mcm/d data)
y_position = 0
y_labels_mcmd = []
y_ticks_mcmd = []
country_sections_mcmd = {}
current_country = None
country_start_y = 0

for idx, (_, plant_info) in enumerate(plants_df_mcmd.iterrows()):
    country = plant_info['country_name']
    plant = plant_info['plant_name']

    # Track country sections
    if country != current_country:
        if current_country is not None:
            country_sections_mcmd[current_country] = (country_start_y, y_position - 1)
        current_country = country
        country_start_y = y_position

    # Get all monthly aggregations for this plant
    plant_months = monthly_df_mcmd[(monthly_df_mcmd['country_name'] == country) & (monthly_df_mcmd['plant_name'] == plant)]

    # Plot each month
    for _, month_data in plant_months.iterrows():
        # Get color for country
        color = primary_colors.get(country, '#666666')

        # Use both actual and visual capacity
        cumulative_capacity = month_data['capacity'] if pd.notna(month_data['capacity']) else 1.0
        visual_capacity = month_data['visual_capacity']
        train_count = month_data['train_count']

        # Better scaling for visual appeal - maintain the original chart's proportions
        base_scale = 0.12  # Similar to original 0.15 scale
        max_bar_height = 0.45  # Allow taller bars but prevent extreme overflow
        bar_height = min(visual_capacity * base_scale, max_bar_height)

        # Create gradient effect for high-capacity bars
        if cumulative_capacity > 50:  # Adjusted threshold for Mcm/d
            # Use gradient for very high capacity bars
            bar_alpha = 0.9
            edge_color = color
            edge_width = 2
        else:
            bar_alpha = 0.85
            edge_color = 'none'
            edge_width = 0

        # Create vertical bar with optimal proportions
        bar_width = 10  # Wider for better visibility and text spacing
        bar = Rectangle((mdates.date2num(month_data['month_start']) - bar_width/2, y_position - bar_height/2),
                       bar_width,
                       bar_height,
                       facecolor=color,
                       edgecolor=edge_color,
                       linewidth=edge_width,
                       alpha=bar_alpha,
                       zorder=5)
        ax2.add_patch(bar)

        # Add subtle white line at base for definition
        ax2.plot([mdates.date2num(month_data['month_start']) - bar_width/2,
                mdates.date2num(month_data['month_start']) + bar_width/2],
               [y_position - bar_height/2, y_position - bar_height/2],
               color='white',
               linewidth=1.5,
               zorder=6)

        # Professional text formatting with clean design
        # Main capacity text above bar
        main_text = f"{cumulative_capacity:.0f}"  # Show as integer for Mcm/d

        # Train count text below bar (only for multiple trains)
        if train_count > 1:
            train_text = f"{train_count}"
        else:
            train_text = None

        # Always position text above the bar for consistency
        text_y = y_position + bar_height/2 + 0.15
        text_color = color
        text_size = 10  # Reduced by 1px as requested
        text_weight = 'bold'

        # Main capacity text - always prominent and readable
        ax2.text(month_data['month_start'], text_y,
                main_text,
                fontsize=text_size,
                ha='center',
                va='center',
                color=text_color,
                weight=text_weight,
                zorder=8)

        # Train count label below bar (only for multiple trains)
        if train_text:
            train_label_y = y_position - bar_height/2 - 0.12  # Position below the bar
            ax2.text(month_data['month_start'], train_label_y,
                    train_text,
                    fontsize=7,
                    ha='center',
                    va='center',
                    color=text_color,
                    weight='normal',
                    alpha=0.9,
                    zorder=8)


    y_labels_mcmd.append(f"{plant}")
    y_ticks_mcmd.append(y_position)

    # Add elegant horizontal line after each plant (except the last one)
    if idx < len(plants_df_mcmd) - 1:
        ax2.axhline(y=y_position + 0.5, color='#E0E0E0', linestyle='-', linewidth=0.5, alpha=0.7, zorder=2)

    y_position += 1

# Add the last country section
if current_country is not None:
    country_sections_mcmd[current_country] = (country_start_y, y_position - 1)

# Add country section backgrounds - aligned with horizontal lines
for idx, (country, (start_y, end_y)) in enumerate(country_sections_mcmd.items()):
    # The horizontal lines are at y + 0.5, so backgrounds should go from one line to the next
    if idx == 0:
        # First country: from top of chart to first horizontal line
        top_boundary = -0.5
    else:
        # Other countries: from previous horizontal line
        top_boundary = start_y - 0.5

    # Bottom boundary is at the horizontal line after the last plant of this country
    bottom_boundary = end_y + 0.5

    # Subtle background shading for country sections
    rect = Rectangle((mdates.date2num(datetime(2025, 1, 1)), top_boundary),
                    mdates.date2num(datetime(2028, 1, 31)) - mdates.date2num(datetime(2025, 1, 1)),
                    bottom_boundary - top_boundary,
                    facecolor=primary_colors.get(country, '#666666'),
                    alpha=0.03,  # Even more subtle
                    edgecolor='none',  # No edge to avoid lines
                    zorder=1)
    ax2.add_patch(rect)

# Add arrows for internal adjustments showing change from Woodmac baseline (reuse same logic)
print("Adding arrows for internal adjustments...")
internal_trains_mcmd = df_mcmd[df_mcmd['data_source'] == 'internal'].copy()

if len(internal_trains_mcmd) > 0:
    # Create a mapping of plant to y_position
    plant_to_y_mcmd = {}
    for i, plant in enumerate(monthly_df_mcmd['plant_name'].unique()):
        # Find the y position for this plant
        for j, y_label in enumerate(y_labels_mcmd):
            if plant in y_label:
                plant_to_y_mcmd[plant] = y_ticks_mcmd[j]
                break

    for _, train in internal_trains_mcmd.iterrows():
        if pd.notna(train['woodmac_date']) and pd.notna(train['internal_date']):
            plant_name = train['plant_name']
            woodmac_date = pd.to_datetime(train['woodmac_date'])
            internal_date = pd.to_datetime(train['internal_date'])

            # Get y position for this plant
            y_pos = plant_to_y_mcmd.get(plant_name)
            if y_pos is not None:
                # Only show arrow if dates are different and within our chart range
                if (woodmac_date != internal_date and
                    pd.Timestamp('2025-01-01') <= woodmac_date <= pd.Timestamp('2028-01-31') and
                    pd.Timestamp('2025-01-01') <= internal_date <= pd.Timestamp('2028-01-31')):

                    # Create arrow from woodmac_date to internal_date
                    arrow_y = y_pos - 0.3  # Slightly below the plant line

                    # Determine arrow color and style based on direction
                    if internal_date < woodmac_date:  # Moved earlier
                        arrow_color = '#2E8B57'  # Sea green for earlier
                        arrow_style = '->'
                        # Arrow FROM internal_date TO woodmac_date (pointing left)
                        arrow_start = mdates.date2num(woodmac_date)
                        arrow_end = mdates.date2num(internal_date)
                    else:  # Moved later
                        arrow_color = '#B22222'  # Fire brick for later
                        arrow_style = '->'
                        # Arrow FROM woodmac_date TO internal_date (pointing right)
                        arrow_start = mdates.date2num(woodmac_date)
                        arrow_end = mdates.date2num(internal_date)

                    # Add the arrow
                    ax2.annotate('',
                               xy=(arrow_end, arrow_y),
                               xytext=(arrow_start, arrow_y),
                               arrowprops=dict(arrowstyle=arrow_style,
                                             color=arrow_color,
                                             lw=2,
                                             alpha=0.8,
                                             shrinkA=0, shrinkB=0),
                               zorder=9)

                    # Add small labels
                    mid_date = woodmac_date + (internal_date - woodmac_date) / 2
                    days_diff = abs((internal_date - woodmac_date).days)
                    months_diff = round(days_diff / 30.44, 1)  # Average days per month

                    # Label showing the change
                    label_text = f"{months_diff:.0f}m"

                    ax2.text(mdates.date2num(mid_date), arrow_y + 0.15,
                           label_text,
                           fontsize=8,
                           ha='center',
                           va='bottom',
                           color=arrow_color,
                           weight='bold',
                           alpha=0.9,
                           zorder=10,
                           bbox=dict(boxstyle="round,pad=0.2",
                                   facecolor='white',
                                   edgecolor=arrow_color,
                                   alpha=0.8,
                                   linewidth=0.5))

print(f"Added {len(internal_trains_mcmd)} internal adjustment arrows")

# Format x-axis - show every month
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%y'))
ax2.xaxis.set_major_locator(mdates.MonthLocator())
ax2.xaxis.set_minor_locator(mdates.MonthLocator())

# Set x-axis limits with padding for country labels
ax2.set_xlim(pd.Timestamp('2025-01-01'), pd.Timestamp('2028-01-31'))

# Remove axis labels
ax2.set_xlabel('', fontsize=12, weight='bold')
ax2.set_ylabel('', fontsize=12, weight='bold')

# Set y-axis
ax2.set_yticks(y_ticks_mcmd)
ax2.set_yticklabels(y_labels_mcmd, fontsize=10)
ax2.set_ylim(-1, len(y_ticks_mcmd))

# Create a mapping of y-position to country for coloring
y_to_country_mcmd = {}
for country, (start_y, end_y) in country_sections_mcmd.items():
    for i in range(int(start_y), int(end_y) + 1):
        y_to_country_mcmd[i] = country

# Color y-axis labels according to their country
ytick_labels_mcmd = ax2.get_yticklabels()
for i, label in enumerate(ytick_labels_mcmd):
    if i in y_to_country_mcmd:
        country = y_to_country_mcmd[i]
        color = primary_colors.get(country, '#666666')
        label.set_color(color)
        label.set_weight('bold')
        label.set_fontsize(11)

# Add vertical grid lines for each month
for year in [2025, 2026, 2027, 2028]:
    for month in range(1, 13):
        try:
            date = pd.Timestamp(f'{year}-{month:02d}-01')
            if pd.Timestamp('2025-01-01') <= date <= pd.Timestamp('2028-01-31'):
                # Major grid lines for January
                if month == 1:
                    ax2.axvline(x=date, color='#666666', linestyle='-', linewidth=1.5, alpha=0.8, zorder=2)
                # Regular grid lines for other months
                else:
                    ax2.axvline(x=date, color='#CCCCCC', linestyle='-', linewidth=0.8, alpha=0.6, zorder=2)
        except:
            break

# Remove horizontal grid
ax2.grid(False, axis='y')

# Add current month marker
current_date = dt.now()
# Get first day of current month
first_of_month = current_date.replace(day=1)
if pd.Timestamp('2025-01-01') <= pd.Timestamp(first_of_month) <= pd.Timestamp('2028-01-31'):
    # Add vertical line for current month
    ax2.axvline(x=first_of_month, color='#E74C3C', linestyle='--', linewidth=1.5, alpha=0.7, zorder=3)

    # Add "Today" label
    ax2.text(first_of_month, ax2.get_ylim()[1] - 0.5, 'Today',
            rotation=0, ha='center', va='bottom',
            fontsize=10, color='#E74C3C', weight='bold',
            bbox=dict(boxstyle="round,pad=0.3", facecolor='white', edgecolor='#E74C3C', alpha=0.8),
            zorder=10)

# Remove top and right spines for cleaner look
ax2.spines['top'].set_visible(False)
ax2.spines['right'].set_visible(False)
ax2.spines['left'].set_color('#CCCCCC')
ax2.spines['bottom'].set_color('#CCCCCC')

# Adjust tick parameters
ax2.tick_params(axis='both', which='major', labelsize=10, colors='#333333')
ax2.tick_params(axis='y', which='major', length=0)  # Remove y-axis tick marks
ax2.tick_params(axis='x', which='major', length=5)  # Keep x-axis tick marks

# Set x-axis label styling - make text larger and clearer
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=0, ha='center', fontsize=9, weight='bold')

# Add country names on the right y-axis
ax2_right = ax2.twinx()  # Create a second y-axis
ax2_right.set_ylim(ax2.get_ylim())  # Match the main y-axis limits

# Calculate middle position for each country and wrap long names
import textwrap

country_labels_mcmd = []
country_positions_mcmd = []
for country, (start_y, end_y) in country_sections_mcmd.items():
    middle_y = (start_y + end_y) / 2
    country_positions_mcmd.append(middle_y)

    # Wrap long country names
    if len(country) > 15:
        wrapped_country = '\n'.join(textwrap.wrap(country, width=15, break_long_words=False))
    else:
        wrapped_country = country
    country_labels_mcmd.append(wrapped_country)

# Set country labels on right axis
ax2_right.set_yticks(country_positions_mcmd)
ax2_right.set_yticklabels(country_labels_mcmd, fontsize=12, fontweight='bold')

# Style the right axis
ax2_right.tick_params(axis='y', which='major', length=0)  # Remove tick marks
ax2_right.spines['right'].set_visible(False)
ax2_right.spines['top'].set_visible(False)
ax2_right.spines['bottom'].set_visible(False)
ax2_right.spines['left'].set_visible(False)
ax2_right.grid(False)  # Remove any grid lines from secondary axis

# Color the country labels
original_countries_mcmd = list(country_sections_mcmd.keys())
for i, (label, country) in enumerate(zip(ax2_right.get_yticklabels(), original_countries_mcmd)):
    label.set_color(primary_colors.get(country, '#666666'))

# Add clean, professional note about chart elements
ax2.text(0.02, 0.02, 'Bar height represents cumulative monthly capacity (Mcm/d) • Number below bar indicates multiple trains in same month • Arrows show internal adjustments vs Woodmac baseline',
        transform=ax2.transAxes, fontsize=9,
        color='#666666', style='italic')

# Adjust layout
plt.tight_layout()
plt.subplots_adjust(bottom=0.08, right=0.88)  # Make room for country labels on right

# Save with high DPI for presentation quality
plt.savefig(output_dir / 'lng_timeline_postgres_mcmd.png', dpi=300, bbox_inches='tight',
            facecolor='white', edgecolor='none')

print(f"Mcm/d Chart created successfully with {len(df_mcmd)} prioritized train records aggregated into {len(monthly_df_mcmd)} monthly entries")
print(f"File saved: {output_dir / 'lng_timeline_postgres_mcmd.png'}")

print("\n" + "="*60)
print("BOTH CHARTS CREATED SUCCESSFULLY!")
print("="*60)
print("Files created:")
print("  • lng_timeline_postgres.png     (MTPA units)")
print("  • lng_timeline_postgres_mcmd.png (Mcm/d units)")
print("Both charts show internal adjustments with correct arrow directions.")
print("="*60)

plt.show()
