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

def calculate_map_stats(
        df: pl.DataFrame,
        surface_selection: list[str],
        year_range: list[int],
        metric: str,
        show_growth: bool
)->pl.DataFrame:
    mapper = {
        "Prix moyen": lambda x: pl.mean(x),
        "Prix médian": lambda x: pl.median(x)
    }
    granularity = ["year", "section"] if show_growth else ["section"]

    stats = (
        df
        .filter(
            pl.col("surface_category").is_in(surface_selection),
            pl.col('year').is_in(year_range),
            pl.col('prix_m2') > 500 # remove absurd prices
        )
        .group_by(granularity)
        .agg(
            mapper[metric]('prix_m2').round(2).cast(int),
            pl.len()
        )
    )
    if not show_growth:
        return stats
    
    return (
        stats
        .filter(pl.col('len') >= 5)
        .sort('section', "year", descending=[True, False])
        .with_columns(
            pl.col("prix_m2").pct_change().over('section').alias(f"croissance").mul(100).round(2)
        )
        .group_by('section')
        .agg(pl.mean('croissance').alias('prix_m2'))
        .drop_nulls()
    )

stats = df.pipe(calculate_map_stats, surface_selection, year_range, metric, show_growth)
housing_prices = {dic["section"]: dic["prix_m2"] for dic in stats.to_dicts()}
polygon_data = {k: v for k, v in polygon_data.items() if k in housing_prices.keys()}



map = plot_map(housing_prices, polygon_data)
st.plotly_chart(map)

#TODO
#Carte avec les sections
# prix par section si selectbox
# années, type de surface, métrique, 
# select section cadastre, all sections and adjacency by default

# if selection, evolution of the section compared to the neighborood, compared to nice average by lenght of surface 