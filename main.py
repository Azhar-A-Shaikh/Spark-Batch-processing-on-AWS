# python3 main.py --config '{"app_id" : "251803cdbb994fe2813635578dacbd0a","s3_out_location":"s3://pysparkapi/api_response/","s3_error_out_location":"s3://pysparkapi/api_response/"}'

import json
import logging
import argparse
import requests
import pandas as pd
import awswrangler as wr
from datetime import datetime


def get_module_logger(mod_name):
    logger = logging.getLogger(mod_name)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


logger = get_module_logger(__name__)


def get_python_day(airflow_timestamp):
    return str(airflow_timestamp)[:10]


def get_partitions(run_date):
    """Get the partition columns from the date"""
    return run_date[:4], run_date[5:7]


def currencyapi(run_date, app_id):
    """Get the response from the openexchangerates API for a specific date"""
    logger.info("Started with API: openexchangerates")
    url = "https://openexchangerates.org/api/historical/{}.json?app_id={}&base=USD".format(
        run_date, app_id
    )
    response = requests.request("GET", url)
    if response.status_code not in (200, 202):
        logger.error("response.status_code = %s", response.status_code)
        logger.error("response.text = %s", response.text)
    logger.info("Done with API: openexchangerates")
    return response.json()


def get_write_location(s3_path, run_date):
    """This function returns the s3 path where you can write your df.
    It takes the s3 path till the partition as an input and append the
    latest partition to the same.
    Sample args: s3://slintel-data/website/company/company_details/"""
    return s3_path + run_date


def parse_response(record, run_date, app_run_ts, year, month):
    """Parse the API response and return a DataFrame"""
    logger.info("Starting on parsing the response")
    rate_details = record.get("rates", [])
    currency_details_sgd = []
    sgd_rate = record["rates"]["SGD"]  # Getting the SGD rate
    for key, value in rate_details.items():
        currency_detail_sgd = {
            "run_date": run_date,
            "target_currency": key,
            "rates_base_sgd": value / sgd_rate,
            "rates_base_usd": value,
            "updated_at": app_run_ts,
            "year": year,
            "month": month,
        }
        currency_details_sgd.append(currency_detail_sgd)
    df_currency_details_sgd = pd.DataFrame(currency_details_sgd)
    logger.info("Done with parsing the response")
    return df_currency_details_sgd


def parse_error_response(record, run_date, app_run_ts, year, month):
    """Parse the API response and return a DataFrame"""
    logger.info("Starting on parsing the error response")
    error_details = []
    error_detail = {
        "run_date": run_date,
        "error": record["error"],
        "status": record["status"],
        "message": record["message"],
        "description": record["description"],
        "updated_at": app_run_ts,
        "year": year,
        "month": month,
    }
    error_details.append(error_detail)
    df_error_details = pd.DataFrame(error_details)
    logger.info("Done with parsing the error response")
    return df_error_details


def main():
    parser = argparse.ArgumentParser()  # Parsen object 
    parser.add_argument("--run_ts", type=str, required=False, default="")
    parser.add_argument("--config", type=str, required=True)
    args = parser.parse_args()
    logger.debug("Done with all the config collection")
    config = json.loads(args.config)
    logger.debug("Config: %s", json.dumps(config, indent=2))
    app_id = config["app_id"]
    s3_out_location = config[""s3://pysparkapi/banktxn/response/"] # Creating a folder to store response result for the api
    s3_error_out_location = config[""s3://pysparkapi/banktxn/error"] # Creating a folder to store the error messages
    if args.run_ts == "":  # When no date ie run_ts is mentioned the program will fetch todays date. 
        logger.debug("args.run_ts is not provided, hence taking the current timestamp")
        run_ts = datetime.now()
    else:
        run_ts = get_python_day(args.run_ts)

    run_date = get_python_day(run_ts)
    year, month = get_partitions(run_date)
    app_run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        record = currencyapi(run_date, app_id)
        df_currency_details_sgd = parse_response(
            record, run_date, app_run_ts, year, month
        )
        # print(df_currency_details_sgd.head())
        if len(df_currency_details_sgd) > 0 and s3_out_location != "":
            s3_out_location = s3_out_location + "response/"
            s3_path_currency_details_sgd = (
                get_write_location(s3_out_location, run_date)
                + "/currency_details_sgd.parquet"
            )
            wr.s3.to_parquet(
                df=df_currency_details_sgd, path=s3_path_currency_details_sgd
            )
            logger.info(
                "Ingested successfully for Date:%s and the record count is: %s",
                run_date,
                len(df_currency_details_sgd),
            )
    except:
        df_error_details = parse_error_response(
            record, run_date, app_run_ts, year, month
        )
        if len(df_error_details) > 0 and s3_error_out_location != "":
            s3_out_location = s3_out_location + "error/"
            s3_path_currency_error_details = (
                get_write_location(s3_out_location, run_date)
                + "/currency_details_sgd_error.parquet"
            )
            wr.s3.to_parquet(df=df_error_details, path=s3_path_currency_error_details)
            logger.error(
                "Encountered with an ERROR!! Error logs ingested for Date:%s and the record count is: %s",
                run_date,
                len(df_error_details),
            )


if __name__ == "__main__":
    main()
