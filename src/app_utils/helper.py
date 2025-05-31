import streamlit as st
from src.core import config
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import json
from shapely.geometry.polygon import Polygon

@st.cache_data
def load_data():
    return pl.read_csv(config.data_dir / "cleaned" / "data_nice_cleaned.csv", try_parse_dates=True)

@st.cache_data
def load_cadastre_data()->dict[str, list]:
    with open(config.data_dir / "cadastre" / "code-coords.json", "r") as f:
        polygon_data = json.load(f)
        polygon_data = {k: Polygon(v) for k, v in polygon_data.items()}
    return polygon_data

def filter_data(
        df: pl.DataFrame,
        housing_type: list[str],
        section: list[str]
)-> pl.DataFrame:
    return df.filter(
        pl.col('section').is_in(section),
        pl.col('type_local').is_in(housing_type),
        pl.col('prix_m2') > 500 # remove absurd prices
    )


def calculate_stats(
        df: pl.DataFrame,
        granularity: list[str] = ['year', 'surface_category']
)-> pl.DataFrame:
    return (
        df
        .group_by(granularity)
        .agg(
            pl.mean('prix_m2').alias('prix_moyen_m2'),
            pl.median('prix_m2').alias('prix_median_m2'),
            pl.len().alias('nb_transactions')
        )
        .sort('surface_category', 'year')
        .with_columns([
            pl.col(col).pct_change().over('surface_category').alias(f"{col}_pct_change").fill_null(0)
            for col in ["prix_moyen_m2", "prix_median_m2"]
        ])
    )

def plot_evolution(
        stats: pl.DataFrame,
        metric: str
)->go.Figure:
    fig = px.line(
        stats,
        x="year", 
        y=metric, 
        color="surface_category",
        hover_data = {
                "nb_transactions": ":.0f", 
                metric: ":.0f",
                f"{metric}_pct_change": ":.0%"
        }
        )
    fig.update_xaxes(
        dtick=1,  # Show every 1 year
        tickmode='linear'
        )
    return fig

def average_price_per_neighborhood(
        df: pl.DataFrame,
        adjacing_sections_df: pl.DataFrame
)->pl.DataFrame:
    return (
        df
        .join(adjacing_sections_df, on = "section")
        .explode(columns="adjacing_sections")
        .drop('section')
        .rename({"adjacing_sections": "section"})
    )

def calculate_price_per_zone(
        df: pl.DataFrame,
        surface_selection: list[str],
        year_range: list[int],
        metric: str,
        granularity: list[str]
)->pl.DataFrame:
    mapper = {
        "Prix moyen": lambda x: pl.mean(x),
        "Prix médian": lambda x: pl.median(x)
    }

    return (
        df
        .filter(
            pl.col("surface_category").is_in(surface_selection),
            pl.col('year').is_between(year_range[0], year_range[1]),
            pl.col('prix_m2') > 500 # remove absurd prices
        )
        .group_by(granularity)
        .agg(
            mapper[metric]('prix_m2').round(2).cast(int),
            pl.len()
        )
    )

def calculate_price_growth(
        df: pl.DataFrame,
        year_range: list[int],
        section_col: str
)->pl.DataFrame:
    return (
        df
        .filter(
            pl.col('len') >= 5,
            pl.col('year').is_in(year_range)
        )
        .sort(section_col, "year", descending=[True, False])
        .with_columns(
            pl.col("prix_m2").pct_change().over(section_col).alias(f"croissance").mul(100).round(2)
        )
        .group_by(section_col)
        .agg(pl.mean('croissance').alias('prix_m2'))
        .drop_nulls()
    )

def plot_map(
        housing_metric: dict[str, float|int],
        polygon_data: dict[str, Polygon]
)->go.Figure:
    price_values = list(housing_metric.values())
    min_price = min(price_values)
    max_price = max(price_values)

    color_choice = "RdYlGn_r"

    # Create the map
    fig = go.Figure()

    # Add each polygon with color based on price
    for name, polygon in polygon_data.items():
        # Extract coordinates
        x_coords, y_coords = polygon.exterior.coords.xy
        
        # Normalize price to 0-1 scale for color mapping
        price = housing_metric[name]
        normalized_price = (price - min_price) / (max_price - min_price)
        
        # Create color (using Viridis colorscale)
        color = px.colors.sample_colorscale(color_choice, normalized_price)[0]
        
        # Add polygon
        fig.add_trace(go.Scattermap(
            lon=list(x_coords),
            lat=list(y_coords),
            mode='lines',
            fill='toself',
            name=f"{name}",  # Clean name only
            line=dict(width=1, color='white'),
            fillcolor=color,
            opacity=0.8,
            hovertemplate=f"<b>{name}</b><br>Price: €{price:,}<extra></extra>",
            showlegend=False  # Hide individual traces from legend
        ))

    # Add a single invisible trace for the colorbar
    fig.add_trace(go.Scattermap(
        lon=[7.2620],  # Nice center
        lat=[43.7102],
        mode='markers',
        marker=dict(
            size=0,  # Invisible marker
            colorscale=color_choice,
            cmin=min_price,
            cmax=max_price,
            colorbar=dict(
                title="Housing Price (€)",
                thickness=15,  # Thinner colorbar
                len=0.7,       # Shorter colorbar
                x=1.02,        # Position to the right
                tickformat=".0f",  # No decimals
                tickprefix="€"     # Euro symbol
            )
        ),
        showlegend=False,
        hoverinfo='skip'
    ))

    # Configure the map with proper controls
    fig.update_layout(
        map=dict(
            style="carto-positron",
            center=dict(lat=43.7502, lon=7.2093),
            zoom=11
        ),
        showlegend=False,  # Clean - no messy legend
        height=600,
        #width=800,
        margin=dict(l=0, r=80, t=50, b=0),  # Space for colorbar
    )

    # Enable all map controls
    fig.update_layout(
        map=dict(
            style="carto-positron",
            center=dict(lat=43.7102, lon=7.2620),
            zoom=12,
            bearing=0,
            pitch=0
        ),
        # Enable zoom, pan, select, etc.
        dragmode='pan'
    )
    return fig


