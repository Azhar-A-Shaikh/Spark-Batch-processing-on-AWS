import logging
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
    """Get the firstdate of the month"""
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


records = ["2015-01-01",
"2015-01-02",
"2015-01-03",
"2015-01-05",
"2015-01-06",
"2015-01-07",
"2015-01-08",
"2015-01-09",
"2015-01-10",
"2015-01-12",
"2015-01-13",
"2015-01-14",
"2015-01-15",
"2015-01-16",
"2015-01-17",
"2015-01-19",
"2015-01-20",
"2015-01-21",
"2015-01-22",
"2015-01-23",
"2015-01-24",
"2015-01-27",
"2015-01-28",
"2015-01-29",
"2015-01-30",
"2015-01-31",
"2015-02-02",
"2015-02-03",
"2015-02-04",
"2015-02-05",
"2015-02-06",
"2015-02-07",
"2015-02-09",
"2015-02-10",
"2015-02-11",
"2015-02-12",
"2015-02-13",
"2015-02-14",
"2015-02-16",
"2015-02-17",
"2015-02-18",
"2015-02-19",
"2015-02-20",
"2015-02-21",
"2015-02-23",
"2015-02-24",
"2015-02-25",
"2015-02-26",
"2015-02-27",
"2015-02-28",
"2015-03-02",
"2015-03-03",
"2015-03-04",
"2015-03-05",
"2015-03-07",
"2015-03-09",
"2015-03-10",
"2015-03-11",
"2015-03-12",
"2015-03-13",
"2015-03-14",
"2015-03-16",
"2015-03-17",
"2015-03-18",
"2015-03-19",
"2015-03-20",
"2015-03-21",
"2015-03-23",
"2015-03-24",
"2015-03-25",
"2015-03-26",
"2015-03-27",
"2015-03-28",
"2015-03-30",
"2015-03-31",
"2015-04-03",
"2015-04-04",
"2015-04-06",
"2015-04-07",
"2015-04-08",
"2015-04-09",
"2015-04-10",
"2015-04-11",
"2015-04-13",
"2015-04-14",
"2015-04-15",
"2015-04-16",
"2015-04-17",
"2015-04-18",
"2015-04-20",
"2015-04-21",
"2015-04-22",
"2015-04-23",
"2015-04-24",
"2015-04-25",
"2015-04-27",
"2015-04-28",
"2015-04-29",
"2015-04-30",
"2015-05-01",
"2015-05-02",
"2015-05-04",
"2015-05-05",
"2015-05-06",
"2015-05-07",
"2015-05-08",
"2015-05-09",
"2015-05-11",
"2015-05-12",
"2015-05-13",
"2015-05-14",
"2015-05-15",
"2015-05-16",
"2015-05-18",
"2015-05-19",
"2015-05-20",
"2015-05-21",
"2015-05-22",
"2015-05-23",
"2015-05-25",
"2015-05-26",
"2015-05-27",
"2015-05-28",
"2015-05-29",
"2015-05-30",
"2015-06-01",
"2015-06-02",
"2015-06-03",
"2015-06-04",
"2015-06-05",
"2015-06-06",
"2015-06-08",
"2015-06-09",
"2015-06-10",
"2015-06-11",
"2015-06-12",
"2015-06-13",
"2015-06-15",
"2015-06-16",
"2015-06-17",
"2015-06-18",
"2015-06-19",
"2015-06-20",
"2015-06-22",
"2015-06-23",
"2015-06-24",
"2015-06-25",
"2015-06-26",
"2015-06-27",
"2015-06-29",
"2015-06-30",
"2015-07-01",
"2015-07-02",
"2015-07-03",
"2015-07-04",
"2015-07-06",
"2015-07-07",
"2015-07-08",
"2015-07-09",
"2015-07-10",
"2015-07-11",
"2015-07-12",
"2015-07-13",
"2015-07-14",
"2015-07-15",
"2015-07-16",
"2015-07-17",
"2015-07-18",
"2015-07-20",
"2015-07-21",
"2015-07-22",
"2015-07-23",
"2015-07-24",
"2015-07-25",
"2015-07-27",
"2015-07-28",
"2015-07-29",
"2015-07-30",
"2015-07-31",
"2015-08-01",
"2015-08-03",
"2015-08-04",
"2015-08-05",
"2015-08-06",
"2015-08-07",
"2015-08-08",
"2015-08-10",
"2015-08-11",
"2015-08-12",
"2015-08-13",
"2015-08-14",
"2015-08-15",
"2015-08-16",
"2015-08-17",
"2015-08-18",
"2015-08-19",
"2015-08-20",
"2015-08-21",
"2015-08-22",
"2015-08-23",
"2015-08-24",
"2015-08-25",
"2015-08-26",
"2015-08-27",
"2015-08-28",
"2015-08-29",
"2015-08-31",
"2015-09-01",
"2015-09-02",
"2015-09-03",
"2015-09-04",
"2015-09-05",
"2015-09-06",
"2015-09-07",
"2015-09-08",
"2015-09-09",
"2015-09-10",
"2015-09-11",
"2015-09-13",
"2015-09-14",
"2015-09-15",
"2015-09-16",
"2015-09-17",
"2015-09-18",
"2015-09-19",
"2015-09-20",
"2015-09-21",
"2015-09-22",
"2015-09-23",
"2015-09-24",
"2015-09-25",
"2015-09-28",
"2015-09-29",
"2015-09-30",
"2015-10-01",
"2015-10-03",
"2015-10-05",
"2015-10-06",
"2015-10-07",
"2015-10-08",
"2015-10-09",
"2015-10-12",
"2015-10-13",
"2015-10-14",
"2015-10-15",
"2015-10-16",
"2015-10-17",
"2015-10-19",
"2015-10-20",
"2015-10-21",
"2015-10-23",
"2015-10-26",
"2015-10-27",
"2015-10-28",
"2015-10-29",
"2015-10-30",
"2015-10-31",
"2015-11-02",
"2015-11-03",
"2015-11-04",
"2015-11-05",
"2015-11-06",
"2015-11-07",
"2015-11-08",
"2015-11-09",
"2015-11-10",
"2015-11-11",
"2015-11-12",
"2015-11-13",
"2015-11-14",
"2015-11-16",
"2015-11-17",
"2015-11-18",
"2015-11-19",
"2015-11-20",
"2015-11-21",
"2015-11-23",
"2015-11-24",
"2015-11-25",
"2015-11-26",
"2015-11-27",
"2015-11-30",
"2015-12-01",
"2015-12-02",
"2015-12-03",
"2015-12-04",
"2015-12-05",
"2015-12-07",
"2015-12-08",
"2015-12-09",
"2015-12-10",
"2015-12-11",
"2015-12-14",
"2015-12-15",
"2015-12-16",
"2015-12-17",
"2015-12-18",
"2015-12-19",
"2015-12-21",
"2015-12-22",
"2015-12-23",
"2015-12-24",
"2015-12-26",
"2015-12-28",
"2015-12-29",
"2015-12-30",
"2015-12-31",
"2016-01-01",
"2016-01-02",
"2016-01-04",
"2016-01-05",
"2016-01-06",
"2016-01-07",
"2016-01-08",
"2016-01-11",
"2016-01-12",
"2016-01-13",
"2016-01-14",
"2016-01-15",
"2016-01-16",
"2016-01-18",
"2016-01-19",
"2016-01-20",
"2016-01-21",
"2016-01-22",
"2016-01-25",
"2016-01-27",
"2016-01-28",
"2016-01-29",
"2016-01-30",
"2016-01-31",
"2016-02-01",
"2016-02-02",
"2016-02-03",
"2016-02-04",
"2016-02-05",
"2016-02-06",
"2016-02-08",
"2016-02-09",
"2016-02-10",
"2016-02-11",
"2016-02-12",
"2016-02-15",
"2016-02-16",
"2016-02-17",
"2016-02-18",
"2016-02-19",
"2016-02-20",
"2016-02-22",
"2016-02-23",
"2016-02-24",
"2016-02-25",
"2016-02-26",
"2016-02-27",
"2016-02-29",
"2016-03-01",
"2016-03-02",
"2016-03-03",
"2016-03-04",
"2016-03-05",
"2016-03-07",
"2016-03-08",
"2016-03-09",
"2016-03-10",
"2016-03-11",
"2016-03-13",
"2016-03-14",
"2016-03-15",
"2016-03-16",
"2016-03-17",
"2016-03-18",
"2016-03-19",
"2016-03-21",
"2016-03-22",
"2016-03-23",
"2016-03-24",
"2016-03-25",
"2016-03-26",
"2016-03-28",
"2016-03-29",
"2016-03-30",
"2016-03-31",
"2016-04-02",
"2016-04-04",
"2016-04-05",
"2016-04-06",
"2016-04-07",
"2016-04-08",
"2016-04-11",
"2016-04-12",
"2016-04-13",
"2016-04-14",
"2016-04-15",
"2016-04-16",
"2016-04-18",
"2016-04-19",
"2016-04-20",
"2016-04-21",
"2016-04-22",
"2016-04-23",
"2016-04-25",
"2016-04-26",
"2016-04-27",
"2016-04-28",
"2016-04-29",
"2016-04-30",
"2016-05-02",
"2016-05-03",
"2016-05-04",
"2016-05-05",
"2016-05-06",
"2016-05-07",
"2016-05-09",
"2016-05-10",
"2016-05-11",
"2016-05-12",
"2016-05-13",
"2016-05-16",
"2016-05-17",
"2016-05-18",
"2016-05-19",
"2016-05-20",
"2016-05-21",
"2016-05-23",
"2016-05-24",
"2016-05-25",
"2016-05-26",
"2016-05-27",
"2016-05-30",
"2016-05-31",
"2016-06-01",
"2016-06-02",
"2016-06-03",
"2016-06-04",
"2016-06-06",
"2016-06-07",
"2016-06-08",
"2016-06-09",
"2016-06-10",
"2016-06-13",
"2016-06-14",
"2016-06-15",
"2016-06-16",
"2016-06-17",
"2016-06-18",
"2016-06-20",
"2016-06-21",
"2016-06-22",
"2016-06-23",
"2016-06-24",
"2016-06-27",
"2016-06-28",
"2016-06-29",
"2016-06-30",
"2016-07-01",
"2016-07-02",
"2016-07-03",
"2016-07-04",
"2016-07-05",
"2016-07-06",
"2016-07-07",
"2016-07-08",
"2016-07-11",
"2016-07-12",
"2016-07-13",
"2016-07-14",
"2016-07-15",
"2016-07-16",
"2016-07-17",
"2016-07-18",
"2016-07-19",
"2016-07-20",
"2016-07-21",
"2016-07-22",
"2016-07-23",
"2016-07-25",
"2016-07-26",
"2016-07-27",
"2016-07-28",
"2016-07-29",
"2016-07-30",
"2016-07-31",
"2016-08-01",
"2016-08-02",
"2016-08-03",
"2016-08-04",
"2016-08-05",
"2016-08-06",
"2016-08-07",
"2016-08-08",
"2016-08-09",
"2016-08-10",
"2016-08-11",
"2016-08-12",
"2016-08-13",
"2016-08-16",
"2016-08-17",
"2016-08-18",
"2016-08-19",
"2016-08-20",
"2016-08-21",
"2016-08-22",
"2016-08-23",
"2016-08-24",
"2016-08-25",
"2016-08-26",
"2016-08-28",
"2016-08-29",
"2016-08-30",
"2016-08-31",
"2016-09-01",
"2016-09-02",
"2016-09-03",
"2016-09-04",
"2016-09-05",
"2016-09-06",
"2016-09-07",
"2016-09-08",
"2016-09-09",
"2016-09-10",
"2016-09-12",
"2016-09-13",
"2016-09-14",
"2016-09-15",
"2016-09-16",
"2016-09-17",
"2016-09-18",
"2016-09-19",
"2016-09-20",
"2016-09-21",
"2016-09-22",
"2016-09-23",
"2016-09-24",
"2016-09-26",
"2016-09-27",
"2016-09-28",
"2016-09-29",
"2016-09-30",
"2016-10-01",
"2016-10-03",
"2016-10-04",
"2016-10-05",
"2016-10-06",
"2016-10-07",
"2016-10-08",
"2016-10-09",
"2016-10-10",
"2016-10-11",
"2016-10-12",
"2016-10-13",
"2016-10-14",
"2016-10-15",
"2016-10-17",
"2016-10-18",
"2016-10-19",
"2016-10-20",
"2016-10-21",
"2016-10-22",
"2016-10-24",
"2016-10-25",
"2016-10-26",
"2016-10-27",
"2016-10-28",
"2016-10-29",
"2016-10-31",
"2016-11-01",
"2016-11-02",
"2016-11-03",
"2016-11-04",
"2016-11-05",
"2016-11-07",
"2016-11-08",
"2016-11-09",
"2016-11-10",
"2016-11-11",
"2016-11-12",
"2016-11-13",
"2016-11-14",
"2016-11-15",
"2016-11-16",
"2016-11-17",
"2016-11-18",
"2016-11-19",
"2016-11-20",
"2016-11-21",
"2016-11-22",
"2016-11-23",
"2016-11-24",
"2016-11-25",
"2016-11-26",
"2016-11-27",
"2016-11-28",
"2016-11-29",
"2016-11-30",
"2016-12-01",
"2016-12-02",
"2016-12-03",
"2016-12-04",
"2016-12-05",
"2016-12-06",
"2016-12-07",
"2016-12-08",
"2016-12-09",
"2016-12-10",
"2016-12-12",
"2016-12-13",
"2016-12-14",
"2016-12-15",
"2016-12-16",
"2016-12-17",
"2016-12-19",
"2016-12-20",
"2016-12-21",
"2016-12-22",
"2016-12-23",
"2016-12-24",
"2016-12-26",
"2016-12-27",
"2016-12-28",
"2016-12-29",
"2016-12-30",
"2016-12-31",
"2017-01-02",
"2017-01-03",
"2017-01-04",
"2017-01-05",
"2017-01-06",
"2017-01-07",
"2017-01-09",
"2017-01-10",
"2017-01-11",
"2017-01-12",
"2017-01-13",
"2017-01-14",
"2017-01-16",
"2017-01-17",
"2017-01-18",
"2017-01-19",
"2017-01-20",
"2017-01-21",
"2017-01-23",
"2017-01-24",
"2017-01-25",
"2017-01-27",
"2017-01-28",
"2017-01-29",
"2017-01-30",
"2017-01-31",
"2017-02-01",
"2017-02-02",
"2017-02-03",
"2017-02-04",
"2017-02-06",
"2017-02-07",
"2017-02-08",
"2017-02-09",
"2017-02-10",
"2017-02-13",
"2017-02-14",
"2017-02-15",
"2017-02-16",
"2017-02-17",
"2017-02-18",
"2017-02-20",
"2017-02-21",
"2017-02-22",
"2017-02-23",
"2017-02-24",
"2017-02-27",
"2017-02-28",
"2017-03-01",
"2017-03-02",
"2017-03-03",
"2017-03-04",
"2017-03-06",
"2017-03-07",
"2017-03-08",
"2017-03-09",
"2017-03-10",
"2017-03-13",
"2017-03-14",
"2017-03-15",
"2017-03-16",
"2017-03-17",
"2017-03-18",
"2017-03-20",
"2017-03-21",
"2017-03-22",
"2017-03-23",
"2017-03-24",
"2017-03-25",
"2017-03-26",
"2017-03-27",
"2017-03-28",
"2017-03-29",
"2017-03-30",
"2017-03-31",
"2017-04-01",
"2017-04-03",
"2017-04-04",
"2017-04-05",
"2017-04-06",
"2017-04-07",
"2017-04-08",
"2017-04-10",
"2017-04-11",
"2017-04-12",
"2017-04-13",
"2017-04-14",
"2017-04-15",
"2017-04-17",
"2017-04-18",
"2017-04-19",
"2017-04-20",
"2017-04-21",
"2017-04-22",
"2017-04-24",
"2017-04-25",
"2017-04-26",
"2017-04-27",
"2017-04-28",
"2017-04-29",
"2017-04-30",
"2017-05-01",
"2017-05-02",
"2017-05-03",
"2017-05-04",
"2017-05-05",
"2017-05-06",
"2017-05-08",
"2017-05-09",
"2017-05-10",
"2017-05-11",
"2017-05-12",
"2017-05-13",
"2017-05-15",
"2017-05-16",
"2017-05-17",
"2017-05-18",
"2017-05-19",
"2017-05-20",
"2017-05-22",
"2017-05-23",
"2017-05-24",
"2017-05-25",
"2017-05-26",
"2017-05-29",
"2017-05-30",
"2017-05-31",
"2017-06-01",
"2017-06-02",
"2017-06-03",
"2017-06-04",
"2017-06-05",
"2017-06-06",
"2017-06-07",
"2017-06-08",
"2017-06-09",
"2017-06-10",
"2017-06-12",
"2017-06-13",
"2017-06-14",
"2017-06-15",
"2017-06-16",
"2017-06-17",
"2017-06-19",
"2017-06-20",
"2017-06-21",
"2017-06-22",
"2017-06-23",
"2017-06-24",
"2017-06-27",
"2017-06-28",
"2017-06-29",
"2017-06-30",
"2017-07-01",
"2017-07-03",
"2017-07-04",
"2017-07-05",
"2017-07-06",
"2017-07-07",
"2017-07-09",
"2017-07-10",
"2017-07-11",
"2017-07-12",
"2017-07-13",
"2017-07-14",
"2017-07-15",
"2017-07-17",
"2017-07-18",
"2017-07-19",
"2017-07-20",
"2017-07-21",
"2017-07-22",
"2017-07-24",
"2017-07-25",
"2017-07-26",
"2017-07-27",
"2017-07-28",
"2017-07-29",
"2017-07-31",
"2017-08-01",
"2017-08-02",
"2017-08-03",
"2017-08-04",
"2017-08-05",
"2017-08-07",
"2017-08-08",
"2017-08-09",
"2017-08-10",
"2017-08-11",
"2017-08-12",
"2017-08-13",
"2017-08-14",
"2017-08-16",
"2017-08-17",
"2017-08-18",
"2017-08-19",
"2017-08-21",
"2017-08-22",
"2017-08-23",
"2017-08-24",
"2017-08-25",
"2017-08-26",
"2017-08-28",
"2017-08-29",
"2017-08-30",
"2017-08-31",
"2017-09-01",
"2017-09-04",
"2017-09-05",
"2017-09-06",
"2017-09-07",
"2017-09-08",
"2017-09-11",
"2017-09-12",
"2017-09-13",
"2017-09-14",
"2017-09-15",
"2017-09-16",
"2017-09-18",
"2017-09-19",
"2017-09-20",
"2017-09-21",
"2017-09-22",
"2017-09-24",
"2017-09-25",
"2017-09-26",
"2017-09-27",
"2017-09-28",
"2017-09-29",
"2017-09-30",
"2017-10-01",
"2017-10-02",
"2017-10-03",
"2017-10-04",
"2017-10-05",
"2017-10-06",
"2017-10-07",
"2017-10-09",
"2017-10-10",
"2017-10-11",
"2017-10-12",
"2017-10-13",
"2017-10-16",
"2017-10-17",
"2017-10-18",
"2017-10-19",
"2017-10-20",
"2017-10-21",
"2017-10-23",
"2017-10-24",
"2017-10-25",
"2017-10-26",
"2017-10-27",
"2017-10-28",
"2017-10-30",
"2017-10-31",
"2017-11-01",
"2017-11-02",
"2017-11-03",
"2017-11-04",
"2017-11-06",
"2017-11-07",
"2017-11-08",
"2017-11-09",
"2017-11-10",
"2017-11-12",
"2017-11-13",
"2017-11-14",
"2017-11-15",
"2017-11-16",
"2017-11-17",
"2017-11-18",
"2017-11-20",
"2017-11-21",
"2017-11-22",
"2017-11-23",
"2017-11-24",
"2017-11-25",
"2017-11-27",
"2017-11-28",
"2017-11-29",
"2017-11-30",
"2017-12-01",
"2017-12-02",
"2017-12-04",
"2017-12-05",
"2017-12-06",
"2017-12-07",
"2017-12-08",
"2017-12-09",
"2017-12-11",
"2017-12-12",
"2017-12-13",
"2017-12-14",
"2017-12-15",
"2017-12-16",
"2017-12-18",
"2017-12-19",
"2017-12-20",
"2017-12-21",
"2017-12-22",
"2017-12-25",
"2017-12-26",
"2017-12-27",
"2017-12-28",
"2017-12-29",
"2017-12-30",
"2017-12-31",
"2018-01-01",
"2018-01-02",
"2018-01-03",
"2018-01-04",
"2018-01-05",
"2018-01-06",
"2018-01-07",
"2018-01-08",
"2018-01-09",
"2018-01-10",
"2018-01-11",
"2018-01-12",
"2018-01-13",
"2018-01-15",
"2018-01-16",
"2018-01-17",
"2018-01-18",
"2018-01-19",
"2018-01-20",
"2018-01-22",
"2018-01-23",
"2018-01-24",
"2018-01-25",
"2018-01-29",
"2018-01-30",
"2018-01-31",
"2018-02-01",
"2018-02-02",
"2018-02-03",
"2018-02-05",
"2018-02-06",
"2018-02-07",
"2018-02-08",
"2018-02-09",
"2018-02-12",
"2018-02-13",
"2018-02-14",
"2018-02-15",
"2018-02-16",
"2018-02-17",
"2018-02-19",
"2018-02-20",
"2018-02-21",
"2018-02-22",
"2018-02-23",
"2018-02-26",
"2018-02-27",
"2018-02-28",
"2018-03-01",
"2018-03-02",
"2018-03-03",
"2018-03-05",
"2018-03-06",
"2018-03-07",
"2018-03-08",
"2018-03-09",
"2018-03-10",
"2018-03-12",
"2018-03-13",
"2018-03-14",
"2018-03-15",
"2018-03-16",
"2018-03-17",
"2018-03-19",
"2018-03-20",
"2018-03-21",
"2018-03-22",
"2018-03-23",
"2018-03-26",
"2018-03-27",
"2018-03-28",
"2018-03-29",
"2018-03-30",
"2018-03-31",
"2018-04-02",
"2018-04-03",
"2018-04-04",
"2018-04-05",
"2018-04-06",
"2018-04-07",
"2018-04-09",
"2018-04-10",
"2018-04-11",
"2018-04-12",
"2018-04-13",
"2018-04-16",
"2018-04-17",
"2018-04-18",
"2018-04-19",
"2018-04-20",
"2018-04-21",
"2018-04-23",
"2018-04-24",
"2018-04-25",
"2018-04-26",
"2018-04-27",
"2018-04-29",
"2018-04-30",
"2018-05-01",
"2018-05-02",
"2018-05-03",
"2018-05-04",
"2018-05-05",
"2018-05-06",
"2018-05-07",
"2018-05-08",
"2018-05-09",
"2018-05-10",
"2018-05-11",
"2018-05-12",
"2018-05-13",
"2018-05-14",
"2018-05-15",
"2018-05-16",
"2018-05-17",
"2018-05-18",
"2018-05-19",
"2018-05-21",
"2018-05-22",
"2018-05-23",
"2018-05-24",
"2018-05-25",
"2018-05-26",
"2018-05-28",
"2018-05-29",
"2018-05-30",
"2018-05-31",
"2018-06-01",
"2018-06-02",
"2018-06-03",
"2018-06-04",
"2018-06-05",
"2018-06-06",
"2018-06-07",
"2018-06-08",
"2018-06-11",
"2018-06-12",
"2018-06-13",
"2018-06-14",
"2018-06-15",
"2018-06-16",
"2018-06-18",
"2018-06-19",
"2018-06-20",
"2018-06-21",
"2018-06-22",
"2018-06-23",
"2018-06-25",
"2018-06-26",
"2018-06-27",
"2018-06-28",
"2018-06-29",
"2018-06-30",
"2018-07-02",
"2018-07-03",
"2018-07-04",
"2018-07-05",
"2018-07-06",
"2018-07-07",
"2018-07-08",
"2018-07-09",
"2018-07-10",
"2018-07-11",
"2018-07-12",
"2018-07-13",
"2018-07-14",
"2018-07-16",
"2018-07-17",
"2018-07-18",
"2018-07-19",
"2018-07-20",
"2018-07-21",
"2018-07-23",
"2018-07-24",
"2018-07-25",
"2018-07-26",
"2018-07-27",
"2018-07-28",
"2018-07-30",
"2018-07-31",
"2018-08-01",
"2018-08-02",
"2018-08-03",
"2018-08-04",
"2018-08-06",
"2018-08-07",
"2018-08-08",
"2018-08-09",
"2018-08-10",
"2018-08-11",
"2018-08-13",
"2018-08-14",
"2018-08-16",
"2018-08-17",
"2018-08-18",
"2018-08-20",
"2018-08-21",
"2018-08-22",
"2018-08-23",
"2018-08-24",
"2018-08-25",
"2018-08-27",
"2018-08-28",
"2018-08-29",
"2018-08-30",
"2018-08-31",
"2018-09-01",
"2018-09-03",
"2018-09-04",
"2018-09-05",
"2018-09-06",
"2018-09-07",
"2018-09-08",
"2018-09-10",
"2018-09-11",
"2018-09-12",
"2018-09-13",
"2018-09-14",
"2018-09-15",
"2018-09-17",
"2018-09-18",
"2018-09-19",
"2018-09-20",
"2018-09-21",
"2018-09-24",
"2018-09-25",
"2018-09-26",
"2018-09-27",
"2018-09-28",
"2018-09-29",
"2018-09-30",
"2018-10-01",
"2018-10-02",
"2018-10-03",
"2018-10-04",
"2018-10-05",
"2018-10-06",
"2018-10-08",
"2018-10-09",
"2018-10-10",
"2018-10-11",
"2018-10-12",
"2018-10-13",
"2018-10-14",
"2018-10-15",
"2018-10-16",
"2018-10-17",
"2018-10-18",
"2018-10-19",
"2018-10-20",
"2018-10-22",
"2018-10-23",
"2018-10-24",
"2018-10-25",
"2018-10-26",
"2018-10-27",
"2018-10-29",
"2018-10-30",
"2018-10-31",
"2018-11-01",
"2018-11-02",
"2018-11-03",
"2018-11-04",
"2018-11-05",
"2018-11-06",
"2018-11-07",
"2018-11-08",
"2018-11-09",
"2018-11-10",
"2018-11-12",
"2018-11-13",
"2018-11-14",
"2018-11-15",
"2018-11-16",
"2018-11-17",
"2018-11-18",
"2018-11-19",
"2018-11-20",
"2018-11-21",
"2018-11-22",
"2018-11-23",
"2018-11-26",
"2018-11-27",
"2018-11-28",
"2018-11-29",
"2018-11-30",
"2018-12-01",
"2018-12-03",
"2018-12-04",
"2018-12-05",
"2018-12-06",
"2018-12-07",
"2018-12-08",
"2018-12-10",
"2018-12-11",
"2018-12-12",
"2018-12-13",
"2018-12-14",
"2018-12-15",
"2018-12-17",
"2018-12-18",
"2018-12-19",
"2018-12-20",
"2018-12-21",
"2018-12-24",
"2018-12-25",
"2018-12-26",
"2018-12-27",
"2018-12-28",
"2018-12-29",
"2018-12-31",
"2019-01-01",
"2019-01-02",
"2019-01-03",
"2019-01-04",
"2019-01-05",
"2019-01-07",
"2019-01-08",
"2019-01-09",
"2019-01-10",
"2019-01-11",
"2019-01-14",
"2019-01-15",
"2019-01-16",
"2019-01-17",
"2019-01-18",
"2019-01-19",
"2019-01-21",
"2019-01-22",
"2019-01-23",
"2019-01-24",
"2019-01-25",
"2019-01-28",
"2019-01-29",
"2019-01-30",
"2019-01-31",
"2019-02-01",
"2019-02-02",
"2019-02-04",
"2019-02-05",
"2019-02-06",
"2019-02-07",
"2019-02-08",
"2019-02-09",
"2019-02-11",
"2019-02-12",
"2019-02-13",
"2019-02-14",
"2019-02-15",
"2019-02-16",
"2019-02-17",
"2019-02-18",
"2019-02-19",
"2019-02-20",
"2019-02-21",
"2019-02-22",
"2019-02-25",
"2019-02-26",
"2019-02-27",
"2019-02-28",
"2019-03-01",
"2019-03-02",
"2019-03-04",
"2019-03-05",
"2022-08-20",
"2022-09-02"]
for run_date in records:
    s3_out_location = "s3://pysparkapi/api_response/"
    year, month = get_partitions(run_date)
    app_run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    app_id = "251803cdbb994fe2813635578dacbd0a" # Get the APP id 
    record = currencyapi(run_date, app_id)
    df_currency_details_sgd = parse_response(record, run_date, app_run_ts, year, month)
    # print(df_currency_details_sgd.head())
    if len(df_currency_details_sgd) > 0 and s3_out_location != "":
        s3_out_location = s3_out_location + "response/"
        s3_path_currency_details_sgd = (
            get_write_location(s3_out_location, run_date)
            + "/currency_details_sgd.parquet"
        )
        wr.s3.to_parquet(df=df_currency_details_sgd, path=s3_path_currency_details_sgd)
        logger.info(
            "Ingested successfully for Date:%s and the record count is: %s",
            run_date,
            len(df_currency_details_sgd),
        )