import polars as pl
from datetime import date
from src.core import config
from pathlib import Path
from caseconverter import snakecase
import json

dvf_column_types = {
    "Identifiant de document": str,
    "Reference document": str,
    "1 Articles CGI": str,
    "2 Articles CGI": str,
    "3 Articles CGI": str,
    "4 Articles CGI": str,
    "5 Articles CGI": str,
    "No disposition": int,
    "Date mutation": date,
    "Nature mutation": str,
    "Valeur fonciere": float,
    "No voie": int,
    "B/T/Q": str,
    "Type de voie": str,
    "Code voie": str,
    "Voie": str,
    "Code postal": str,
    "Commune": str,
    "Code departement": str,
    "Code commune": str,
    "Prefixe de section": str,
    "Section": str,
    "No plan": str,
    "No Volume": str,
    "1er lot": str,
    "Surface Carrez du 1er lot": float,
    "2eme lot": str,
    "Surface Carrez du 2eme lot": float,
    "3eme lot": str,
    "Surface Carrez du 3eme lot": float,
    "4eme lot": str,
    "Surface Carrez du 4eme lot": float,
    "5eme lot": str,
    "Surface Carrez du 5eme lot": float,
    "Nombre de lots": int,
    "Code type local": int,
    "Type local": str,
    "Identifiant local": str,
    "Surface reelle bati": int,
    "Nombre pieces principales": int,
    "Nature culture": str,
    "Nature culture speciale": str,
    "Surface terrain": int
}

def load_dvf(file_path: Path)->pl.DataFrame:
    df = (
        pl
        .read_csv(
            file_path,
            separator="|",
            schema_overrides=dvf_column_types,
            decimal_comma=True,
            try_parse_dates=True
        )
    )
    return df.rename({col: snakecase(col) for col in df.columns})

def load_dvf_for_year(year: int) -> pl.DataFrame:
    dvf_files = (config.data_dir / "dvf-data").glob('*.csv')
    file_path = [c for c in dvf_files if str(year) in c.__str__()]
    assert len(file_path) == 1, f"wrong year of multiple results in the folder, list of files detected: {file_path}"
    file_path = file_path[0]
    return load_dvf(file_path)


def load_dvf_years(
    years: list[int] | int = None, 
    transform: callable= None
) -> pl.DataFrame:
    """Load and optionally transform DVF data for multiple years."""
    if type(years) == int:
        years = [years]
    years = years or list(range(2020, 2025))
    
    dataframes = [
        load_dvf_for_year(year).pipe(transform) if transform else load_dvf_for_year(year)
        for year in years
    ]
    
    return pl.concat(dataframes)

def load_json(file_path: Path)->dict:
    with open(file_path, "r") as f:
        json_file = json.load(f)
    return json_file