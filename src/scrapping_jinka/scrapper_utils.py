from src.core import config
import requests
from logzero import logger
from typing import Tuple
import time
from pathlib import Path
import pandas as pd
import json
import os
from retrying import retry
import shutil

def initialize_header(access_token: str) -> dict:
    headers = {
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
    'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {access_token}',
    'Origin': 'https://www.jinka.fr',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Sec-GPC': '1',
    'If-None-Match': 'W/f46-qWZd5Nq9sjWAv9cj3oEhFaxFuek',
    'TE': 'Trailers',
    }
    return headers

@retry(stop_max_attempt_number=3)
def call_api_with_timeout(session, login_url: str, login_dict: dict):
    response = session.post(login_url, login_dict, timeout=5)
    response.raise_for_status()
    return response


def authentificate() -> tuple[requests.sessions.Session, dict]:
    session = requests.Session()
    login_url = "https://api.jinka.fr/apiv2/user/auth"
    login_dict = {
    "email": config.jinka_email,
    "password": config.jinka_password
    }
    logger.info("Trying authentification")
    response = call_api_with_timeout(session,
                                     login_url,
                                     login_dict
                                     )
    access_token = response.json()['access_token']

    if response.status_code == 200:
        logger.info('Authentification succeeded (200)')
        access_token = response.json()['access_token']

    else:
        logger.critical(f'Authentification failed with error {response.status_code}')
        return None, None

    headers = initialize_header(access_token = access_token)

    return session, headers


def save_alerts_description(alerts_json):
    logger.info("Saving alert description")
    save_folder = config.data_dir / "raw_jinka"

    for alert in alerts_json:
        alert_id = alert['id']
        alert_folder = save_folder / f"alert_{alert_id}"

        alert_folder.mkdir(exist_ok=True)

        file_name = "description.json"

        with open(alert_folder / file_name, "w") as json_file:
            json.dump(alert, json_file, indent=4)


def get_alerts_id(session: requests.sessions.Session, headers: dict) -> dict:
    alerts_json = session.get('https://api.jinka.fr/apiv2/alert', headers=headers).json()
    logger.info(f"get {len(alerts_json)} id")
    alerts = {alert['id']: alert['search_type'] for alert in alerts_json}
    return alerts, alerts_json

def get_api_url(id: str, page_number: int) -> str:
    return f'https://api.jinka.fr/apiv2/alert/{id}/dashboard?filter=all&page={page_number}' 


def get_json_per_alert(session: requests.sessions.Session, headers:dict, alerts: dict, k:int = 2) -> dict:
    dict_json_pages = dict()

    for alert_id in alerts.keys():
        logger.info(f'Processing alert {alert_id}')
        json_pages = []

        for i in range(1, k + 1):
            try:
                url = get_api_url(alert_id, i)
                logger.info(f'Fetching page {i}')
                response = session.get(url, headers=headers).json()['ads']
                json_pages.append(response)
            except:
                logger.warn('Connection interrupted by Jinka. Waiting 30 seconds before retrying.')
                time.sleep(30)
                logger.warn('Retrying to establish the connection...')
                response = session.get(url, headers=headers).json()['ads']
                json_pages.append(response)

        dict_json_pages[alert_id] = json_pages

    return dict_json_pages


def save_json(dict_json_pages: dict):
    logger.info("Saving files")
    save_folder = config.data_dir / "raw_jinka"

    for alert_id in dict_json_pages.keys():
        alert_folder = save_folder / f"alert_{alert_id}"

        for i, file in enumerate(dict_json_pages[alert_id]):
            file_name = f"page_{i + 1}.json"

            with open(alert_folder / file_name, "w") as json_file:
                json.dump(file, json_file, indent=4)
