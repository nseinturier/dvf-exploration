import streamlit as st
from src.core import config
from src.app_utils.helper import *
from src.loader import load_json
import polars as pl

# Remove default padding
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

def centered_subheader(text):
    st.markdown(f"<h3 style='text-align: center;'>{text}</h3>", unsafe_allow_html=True)


df = load_data()
polygon_data = load_cadastre_data()
adjacing_sections = load_json(config.data_dir / "cadastre" / "adjency_cadastre.json")
sections = df.get_column('section').unique().sort().to_list()

st.title("Price evolution overview")

col1, _, col3 = st.columns(3)
with col1:
    housing_type = st.radio(
        "Type de logement:",
        ["Appartement", "Maison", "Les deux"],
        horizontal=True
    )
    housing_type = [housing_type] if housing_type != "Les deux" else ["Appartement", "Maison"]

with col3:
    st.write("&nbsp;", unsafe_allow_html=True) #blank space to align sections
    include_adjacency = st.checkbox("Inclure les sections cadastres adjacentes")

col1, col2 = st.columns(2)
with col1:
    choosen_section = st.selectbox('Choose a section:', sections, index=sections.index('LC')) 
    section_choice = adjacing_sections[choosen_section] if include_adjacency else [choosen_section]

with col2:
    adjencing_polygons = {k: v for k,v in polygon_data.items() if k in section_choice}
    housing_metric = {c: 1 if c != choosen_section else 2 for c in section_choice}
    centroid = adjencing_polygons[choosen_section].centroid
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

filtered_df = df.pipe(filter_data, housing_type, section_choice)
section_stats = filtered_df.pipe(calculate_stats)
st.markdown("<br><br>", unsafe_allow_html=True)
col1, col2 = st.columns(2)

# Left column content
with col1:
    metric_left = "prix_moyen_m2"
    centered_subheader("Prix moyen au m2")
    fig_left = plot_evolution(section_stats, metric_left)
    st.plotly_chart(fig_left)
    
# Right column content
with col2:
    metric_right = "prix_median_m2"
    centered_subheader("Prix médian au m2")
    fig_right = plot_evolution(section_stats, metric_right)
    st.plotly_chart(fig_right)

st.subheader("Détail des transactions")

col1, col2  = st.columns([2, 1])  # Middle column for slider
with col1:
    surface_types = ["≤25m²", "26-40m²", "41-60m²", "61-80m²", "81-120m²", ">120m²"]
    surface_selection = st.radio('Surface range:', surface_types, index=surface_types.index('61-80m²'), horizontal=True)


with col2:
    min_year = 2020
    max_year = 2024
    year_range = st.slider(
        "Select year range:",
        min_value=min_year,
        max_value=max_year,
        value=(max_year, max_year),
        step=1
    )


transactions_to_show = (
    filtered_df
    .filter(
        pl.col('surface_category') == surface_selection,
        pl.col('year').is_between(year_range[0], year_range[1])
    )
    .with_columns(
        pl.col('date_mutation').cast(str)
    )
    .with_columns(pl.col('prix_m2').round(0))
    .sort("section", 'year', "prix_m2", descending=[False, True, True])
    .select(
        "section", "date_mutation", "prix_m2", "surface_reelle_bati", "valeur_fonciere","parcelle",
        "type_local", "nombre_pieces_principales", "voie"
    )
    #.sort("section", 'date_mutation', "prix_m2", descending=[False, True, True])
)

nb_transactions = len(transactions_to_show)
st.write(f"{nb_transactions} transactions for years {year_range[0]}-{year_range[1]} on {surface_selection} surfaces")

list_parcelles = transactions_to_show.get_column('parcelle').unique().sort().to_list()

with col1:
    col_small, _ = st.columns([1, 2])
    with col_small:
        parcelle_options = ["All parcelles"] + list_parcelles
        parcelle_choice = st.selectbox('Optional: select a parcel:', parcelle_options, index=parcelle_options.index('All parcelles'))
        #parcelle_choice = st.selectbox('Optional: select a parcel:', list_parcelles)

if parcelle_choice != "All parcelles":
    transactions_to_show = transactions_to_show.filter(pl.col('parcelle') == parcelle_choice)
    
st.dataframe(transactions_to_show)


