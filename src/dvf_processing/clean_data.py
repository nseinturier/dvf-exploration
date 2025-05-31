import polars as pl
from src.loader import load_dvf_years
from src.core import config

info_cols = [
    #"date_mutation",
    "nature_mutation",
    #"valeur_fonciere",
    "commune",
    "section",
    "type_local",
    "nombre_pieces_principales",
    "voie",
    "surface_category"
]

agg_cols = [
    "surface_reelle_bati"
]

commune = "NICE"

def pre_treatment(df: pl.DataFrame)->pl.DataFrame:
    return (
        df
        .filter(pl.col('commune') == commune)
        .with_columns(
            prix_m2 = pl.col('valeur_fonciere') / pl.col('surface_reelle_bati'),
            parcelle = pl.concat_str("section", "no_plan")
        )
    )

def create_breaks(df: pl.DataFrame, surface_col: str = "surface_reelle_bati")->pl.DataFrame:
    return (
        df.with_columns(
            pl.col(surface_col).cut(
                breaks=[25, 40, 60, 80, 120], 
                labels=["≤25m²", "26-40m²", "41-60m²", "61-80m²", "81-120m²", ">120m²"]
            ).alias("surface_category")
        )
    )

def clean_data(df: pl.DataFrame)-> pl.DataFrame:
    return (
        df
        .filter(
            pl.col('commune') == commune,
            pl.col('type_local').is_in(["Appartement", "Maison"]),
            pl.col('nature_mutation') == "Vente",
            pl.col('nature_culture').is_null()
        )
        .group_by("date_mutation", 'parcelle', "valeur_fonciere")
        .agg(
            *[pl.first(col).name.keep() for col in info_cols],
            *[pl.sum(col).name.keep() for col in agg_cols],
            *[pl.len().alias("nb_lots")]
        )
        .with_columns(
            prix_m2 = pl.col('valeur_fonciere') / pl.col('surface_reelle_bati'),
            year = pl.col('date_mutation').dt.year()
        )
    )

if __name__== "__main__":
    df = load_dvf_years(transform=pre_treatment).pipe(create_breaks)
    df = df.pipe(clean_data)
    df.write_csv(config.data_dir / "cleaned" /  "data_nice_cleaned.csv")