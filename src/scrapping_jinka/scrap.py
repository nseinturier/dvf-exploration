from src.scrapping_jinka import scrapper_utils as utils
from src.scrapping_jinka.process_data import create_df_from_raw, filter_nice_rent_data
from src.core import config

def scrap_jinka():
    session, headers = utils.authentificate()
    alerts, alerts_json = utils.get_alerts_id(session, headers)
    utils.save_alerts_description(alerts_json)
    jsons = utils.get_json_per_alert(
        session = session,
        headers = headers,
        alerts = alerts,
        k = 10
    )
    utils.save_json(jsons)

if __name__ == "__main__":
    scrap_jinka()
    df = create_df_from_raw()
    filtered_df = df.pipe(filter_nice_rent_data)
    filtered_df.write_csv(config.data_dir / "jinka-csv" / "rent_nice_jinka.csv")