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



map = plot_map(housing_prices, polygon_data)
st.plotly_chart(map)

################################ PART 2 #######################################

#TODO
#Carte avec les sections
# prix par section si selectbox
# années, type de surface, métrique, 
# select section cadastre, all sections and adjacency by default

# if selection, evolution of the section compared to the neighborood, compared to nice average by lenght of surface 