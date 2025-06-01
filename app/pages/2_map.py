import streamlit as st
from src.core import config
from src.app_utils.helper import *
from src.loader import load_json
import polars as pl
import plotly.graph_objects as go
import plotly.express as px
import numpy as np

st.markdown("""
    <style>
        .block-container {
            max-width: 85%;
            padding-top: 3rem;
            padding-right: 1rem;
            padding-left: 1rem;
            padding-bottom: 3rem;
        }
    </style>
""", unsafe_allow_html=True)

df = load_data()
polygon_data = load_cadastre_data()
adjacing_sections = load_json(config.data_dir / "cadastre" / "adjency_cadastre.json")
adjacing_sections_df = pl.DataFrame({
    "section": adjacing_sections.keys(),
    "adjacing_sections": adjacing_sections.values(),
})
sections = df.get_column('section').unique().sort().to_list()

col1, col2, col3, col4 = st.columns(4)
with col1:
    surface_types = ["≤25m²", "26-40m²", "41-60m²", "61-80m²", "81-120m²", ">120m²"]
    surface_selection = st.multiselect('Surface range:', surface_types, default=["61-80m²"])

with col2:
    metric = st.radio(
        "Métrique:",
        ["Prix moyen", "Prix médian"],
        horizontal=True
    )

with col3:
    st.write("&nbsp;", unsafe_allow_html=True) #blank space to align sections
    show_growth = st.checkbox("Show average growth rate")

with col4:
    min_year = 2020
    max_year = 2024
    year_range = st.slider(
        "Select year range:",
        min_value=min_year,
        max_value=max_year,
        value=(2023, max_year),
        step=1
    )
smooth_price = st.checkbox('Smooth price with adjencing neighborhoods:')
df_stats = df.pipe(average_price_per_neighborhood, adjacing_sections_df) if smooth_price else df

granularity = ["year", "section"] if show_growth else ["section"]
stats = calculate_price_per_zone(df_stats, surface_selection, year_range, metric, granularity)
stats = stats.pipe(calculate_price_growth, year_range, "section") if show_growth else stats

housing_prices = {dic["section"]: dic["prix_m2"] for dic in stats.to_dicts()}
polygon_data = {k: v for k, v in polygon_data.items() if k in housing_prices.keys()}



map = plot_map(housing_prices, polygon_data, display_section_name=True)
st.plotly_chart(map)

################################ PART 2 #######################################
st.markdown("<br><br>", unsafe_allow_html=True)
col1, col2 = st.columns(2)
with col1:
    section_choice = st.selectbox('Choose a section:', sections, index=sections.index('LC'))
with col2:
    adjencing_list = adjacing_sections[section_choice]
    adjencing_polygons = {k: v for k,v in polygon_data.items() if k in adjencing_list}
    housing_metric = {c: 1 if c != section_choice else 2 for c in adjencing_list}
    centroid = adjencing_polygons[section_choice].centroid
    lat = centroid.y
    lon = centroid.x
    fig = plot_map(
        housing_metric, 
        adjencing_polygons,
        lon = lon,
        lat = lat,
        height=200,
        width=300,
        display_section_name=True,
        zoom=11.8,
        show_colorbar=False
    )
    st.plotly_chart(fig)

def map_calculate_stats_sections(
        df: pl.DataFrame,
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

stats_section = map_calculate_stats_sections(df, year_range, surface_selection, section_choice)
#st.dataframe(stats_section)

import plotly.express as px
fig = px.bar(stats_section, 
             x='price_type', 
             y='price',
             color='section_type',
             barmode='group',
             text='price',
             title='Mean and Median Prices by Section Type')
fig.update_traces(textposition='outside', 
                  texttemplate='<b>€%{text}</b>')
st.plotly_chart(fig)


#TODO
#map sur la droite avec la selection
# courbes de tendance