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

stats_section = map_calculate_stats_sections(df, adjacing_sections,year_range, surface_selection, section_choice)
#st.dataframe(stats_section)

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

evolution_sections = map_calculate_evolution(df, section_choice, adjacing_sections, surface_selection)
col1, col2 = st.columns(2)
with col1:
    metric_left = "mean_price_m2"
    centered_subheader("Prix moyen au m2") 
    fig_left = plot_evolution(evolution_sections, metric_left, "section_type")
    st.plotly_chart(fig_left)
    
# Right column content
with col2:
    metric_right = "median_price_m2"
    centered_subheader("Prix médian au m2")
    fig_right = plot_evolution(evolution_sections, metric_right, "section_type")
    st.plotly_chart(fig_right)

#TODO
#map sur la droite avec la selection
# courbes de tendance