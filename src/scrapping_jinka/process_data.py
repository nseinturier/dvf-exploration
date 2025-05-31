from pathlib import Path
import pandas as pd
import polars as pl
from src.core import config
from glob import glob
import json
import shutil
from logzero import logger

RELEVANT_COLS = [
    "uuid",
    'search_type',
    "rent",
    'area',
    'room',
    'bedroom',
    'floor',
    'type',
    'city',
    'postal_code',
    'lat',
    'lng',
    'furnished',
    'description',
    'created_at',
    'alert_id',
    'features'
]

def get_json_pages()-> list:
    raw_jinka_files = config.data_dir / "raw_jinka"
    pages_path = [c for c in raw_jinka_files.glob('**/*.json') if 'description' not in c.__str__()]
    return [json.load(open(file_path)) for file_path in pages_path]


def create_df_from_page(page: list):
    df_list = [pd.DataFrame.from_dict(file, orient = 'index').T for file in page]
    df = pd.concat(df_list)
    df = df[RELEVANT_COLS].reset_index(drop = True)

    # process features dict
    expanded_features = pd.json_normalize(df['features'])
    df = df.drop('features', axis = 1)
    df = pd.concat([df, expanded_features], axis=1)
    return df


def filter_nice_rent_data(df: pd.DataFrame):
    return (
        pl.from_pandas(df)
        .filter(
            pl.col('city') == "Nice",
            pl.col('search_type') == "for_rent"
        )
    )

def create_df_from_raw()-> pd.DataFrame:
    logger.info('Create DataFrame')
    pages = get_json_pages()
    dfs = [create_df_from_page(page) for page in pages if len(page) > 0]
    df = pd.concat(dfs).reset_index(drop = True)
    df['link'] = "https://api.jinka.fr/alert_result_view_ad?ad=" + df['id'].astype(str) + "&alert_token=" + df['alert_id'].astype(str)
    return df

def save_df(df: pd.DataFrame):
    logger.info('Save DataFrame')
    file_path = config.data_dir / "jinka_csv" / 'data.csv'
    
    if file_path.exists():
        #incremental
        df_old = pd.read_csv(file_path)
        df = pd.concat([df, df_old]).drop_duplicates().reset_index(drop = True)

        #move old file
        nb_old_file = len(glob('data/interim/old/*'))
        old_file_name = f"data_{nb_old_file + 1}.csv"
        old_file_path = config.data_dir / "old" / old_file_name
        shutil.move(file_path, old_file_path)

    df.to_csv(file_path, index = False)
