import streamlit as st
from src.core import config
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import json
from shapely.geometry.polygon import Polygon

METRIC_MAPPER = {
        "Prix moyen": lambda x: pl.mean(x),
        "Prix médian": lambda x: pl.median(x)
    }

def centered_subheader(text):
    st.markdown(f"<h3 style='text-align: center;'>{text}</h3>", unsafe_allow_html=True)

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
        metric: str,
        color_col: str
)->go.Figure:
    fig = px.line(
        stats,
        x="year", 
        y=metric, 
        color=color_col,
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
    return (
        df
        .filter(
            pl.col("surface_category").is_in(surface_selection),
            pl.col('year').is_between(year_range[0], year_range[1]),
            pl.col('prix_m2') > 500 # remove absurd prices
        )
        .group_by(granularity)
        .agg(
            METRIC_MAPPER[metric]('prix_m2').round(2).cast(int),
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
        polygon_data: dict[str, Polygon],
        lon: float = 7.2620,
        lat: float = 43.7102,
        height: int = 600,
        width: int = None,
        display_section_name: bool = False,
        zoom: int = 12,
        show_colorbar: bool = True
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
        normalized_price = (price - min_price) / (max_price - min_price) if (max_price - min_price) != 0 else 2
        
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

        if display_section_name:
            centroid = polygon.centroid
            fig.add_trace(go.Scattermap(
                lon=[centroid.x],
                lat=[centroid.y],
                mode='text',
                text=[name],
                textposition="middle center",
                textfont=dict(
                    size=12,
                    weight="bold",
                    color='black'  # Try bright color to see if it appears
                ),
                showlegend=False,
                hoverinfo='skip'
            ))

    if show_colorbar:
        fig.add_trace(go.Scattermap(
            lon=[lon],  # Nice center
            lat=[lat],
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
            center=dict(lat=lat, lon=lon),
            zoom=zoom,
            bearing=0,
            pitch=0
        ),
        showlegend=False,  # Clean - no messy legend
        height=height,
        width=width,
        margin=dict(l=0, r=0, t=0, b=0),  # Space for colorbar
        dragmode='pan'
    )
    return fig

def map_calculate_stats_sections(
        df: pl.DataFrame,
        adjacing_sections: dict[str, list],
        year_range: list[int],
        surface_selection: list[str],
        section_choice: list[str],
)->pl.DataFrame:
    adjacing_sections_filtered = [c for c in adjacing_sections[section_choice] if c != section_choice]
    df_filtered = (
        df
        .filter(
            pl.col("surface_category").is_in(surface_selection),
            pl.col('year').is_between(year_range[0], year_range[1]),
            pl.col('prix_m2') > 500 # remove absurd prices
        )
        .with_columns(
            pl
            .when(pl.col('section') == section_choice).then(pl.lit("choosen_section"))
            .when(pl.col('section').is_in(adjacing_sections_filtered)).then(pl.lit('adjacing_section'))
            .otherwise(pl.lit('other_section'))
            .alias('section_type')
        )
    )
    df_all = pl.concat([
        df_filtered.with_columns(pl.lit('other_section').alias('section_type')),
        df_filtered.filter(pl.col('section_type').is_in(["choosen_section", "adjacing_section"]))
    ])

    stats = (
        df_all
        .group_by('section_type')
        .agg(
            mean_price_m2 = pl.median('prix_m2').round(0).cast(int),
            median_price_m2 = pl.mean('prix_m2').round(0).cast(int),
            len = pl.len()
        )
    )

    return (
        stats
        .drop('len')
        .unpivot(
            index = ["section_type"],
            variable_name='price_type',
            value_name="price"
        )
        .sort('section_type', "price_type")
    )

def map_calculate_evolution(
        df: pl.DataFrame,
        section_choice: str,
        adjacing_sections: dict[str, list],
        surface_selection: list[str]
)->pl.DataFrame:
        adjacing_sections_filtered = [c for c in adjacing_sections[section_choice] if c != section_choice]

        df_filtered = (
                df
                .filter(
                        pl.col("surface_category").is_in(surface_selection),
                        pl.col('prix_m2') > 500 # remove absurd prices
                )
                .with_columns(
                        pl
                        .when(pl.col('section') == section_choice).then(pl.lit("choosen_section"))
                        .when(pl.col('section').is_in(adjacing_sections_filtered)).then(pl.lit('adjacing_section'))
                        .otherwise(pl.lit('other_section'))
                        .alias('section_type')
                        )
        )

        evolution_sections = pl.concat([
                df_filtered.with_columns(pl.lit('other_section').alias('section_type')),
                df_filtered.filter(pl.col('section_type').is_in(["choosen_section", "adjacing_section"]))
        ])

        evolution_sections = (
                evolution_sections
                .group_by("year", 'section_type')
                .agg(
                mean_price_m2 = pl.median('prix_m2').round(0).cast(int),
                median_price_m2 = pl.mean('prix_m2').round(0).cast(int),
                nb_transactions = pl.len()
                )
                .sort('section_type', 'year')
                .with_columns([
                pl.col(col).pct_change().over('section_type').alias(f"{col}_pct_change").fill_null(0)
                for col in ["mean_price_m2", "median_price_m2"]
                ])
                .sort('section_type', 'year')
        )

        return evolution_sections