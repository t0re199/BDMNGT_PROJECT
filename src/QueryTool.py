import os
import sys
import time

from datetime import datetime
from datetime import timedelta

from optparse import OptionParser

import numpy as np
import pandas as pd

from pyspark.sql import SparkSession

import mongo.MongoUtils as MongoUtils


__PROJECT_ROOT__ = "../"
__DATA_PATH__ = os.path.join(__PROJECT_ROOT__, "data")

__DATASET_NAME__ = "covid19_full_filtered.csv"
__DATASET_PATH__ = os.path.join(__DATA_PATH__, __DATASET_NAME__)


__APP_NAME__ = "QueryTool"
__SPARK_LOG_LEVEL__ = "WARN"


__DATE_FORMAT__ = "%Y-%m-%d"


def init_spark():
    return SparkSession.builder\
                       .master("local[*]")\
                       .appName(__APP_NAME__)\
                       .getOrCreate()


def date_add_days(str_date, days, date_format=__DATE_FORMAT__):
    return (datetime.strptime(str_date, date_format).date() + timedelta(days=days)).strftime(date_format)


def country_day_increment_by_date(rdd, date):
    return rdd.filter(lambda row: row.date == date)\
              .map(lambda row: (row.country, row.new_cases))\
              .reduceByKey(lambda acc, value: acc + value)\
              .collect()


# aka query 0
def country_new_cases_avg_by_month(rdd, month, date_format=__DATE_FORMAT__):
    return rdd.filter(lambda row: datetime.strptime(row.date, date_format).date().month == month)\
              .map(lambda row: (row.country, (row.new_cases, 0x1)))\
              .reduceByKey(lambda acc, value: (acc[0x0] + value[0x0], acc[0x1] + value[0x1]))\
              .map(lambda agg: {"country": agg[0x0],
                                "new_cases_avg": agg[0x1][0x0] / agg[0x1][0x1] if agg[0x1][0x1] != 0x0 else 0x0,
                                "month": month})\
              .collect()


# aka query 1
def country_day_weeks_new_case_increment_with_new_cases_by_month(rdd, month_date,
                                                                 convert_to_obj=True,
                                                                 date_format=__DATE_FORMAT__):
    if convert_to_obj:
        month_date = datetime.strptime(month_date, date_format).date()

    filtered_rdd = rdd.filter(lambda row: datetime.strptime(row.date, date_format).date().month == month_date.month)\
                      .cache()

    weeks_rdd = filtered_rdd.cartesian(filtered_rdd)\
                            .filter(lambda row: (row[0x0].country == row[0x1].country and
                                                 (date_add_days(row[0x0].date, 0x7) == row[0x1].date or
                                                 date_add_days(row[0x0].date, 0xe) == row[0x1].date)
                                                ))\
                            .map(lambda row: ((row[0x0].country, row[0x0].date), (row[0x1].new_deaths, 0x0, 0x0))
                                                if date_add_days(row[0x0].date, 0x7) == row[0x1].date
                                                else ((row[0x0].country, row[0x0].date), (0x0, row[0x1].new_deaths, 0x0)))

    return filtered_rdd.map(lambda row: ((row.country, row.date), (0x0, 0x0, row.new_cases)))\
                       .union(weeks_rdd)\
                       .reduceByKey(lambda acc, value: (acc[0x0] + value[0x0], acc[0x1] + value[0x1], acc[0x2] + value[0x2]))\
                       .map(lambda agg: {"country": agg[0x0][0x0],
                                         "date": agg[0x0][0x1],
                                         "next_weeks_new_deaths_ratio": agg[0x1][0x0] / agg[0x1][0x1] if agg[0x1][0x1] != 0x0 else 0x0,
                                         "new_cases": agg[0x1][0x2]})\
                       .collect()


# aka query 2
def country_recovered_wrt_new_case_until_date(rdd, country, date, convert_to_obj=True, date_format=__DATE_FORMAT__):
    if convert_to_obj:
        date = datetime.strptime(date, date_format).date()

    return rdd.filter(lambda row: row.country == country and datetime.strptime(row.date, date_format).date() <= date)\
              .map(lambda row: (row.country, (row.new_cases, row.new_recovered)))\
              .reduceByKey(lambda acc, value: (acc[0x0] + value[0x0], acc[0x1] + value[0x1]))\
              .map(lambda agg: {"country": country,
                                "date": date.strftime(date_format),
                                "cumulative_recovered_new_cases_ratio": agg[0x1][0x1] / agg[0x1][0x0] if agg[0x1][0x0] != 0x0 else 0x0})\
              .collect()


def exec_country_new_cases_avg_by_month(rdd, month, date_format=__DATE_FORMAT__):
    print("[*] Starting Query #0.")
    elapsed_time = -time.time()
    spark_data = country_new_cases_avg_by_month(rdd, month, date_format)
    elapsed_time += time.time()
    print("[*] Completed, {:1.2f}s required.".format(np.round(elapsed_time, 0x2)))
    print("[*] Dumping in MongoDB.")
    MongoUtils.insert_many_to(MongoUtils.DB_NAME,
                              MongoUtils.COLLECTIONS_NAMES[0x0],
                              spark_data)
    print("[*] Done.")


def exec_country_day_weeks_new_case_increment_with_new_cases_by_month(rdd, month,
                                                                      convert_to_obj=True,
                                                                      date_format=__DATE_FORMAT__):
    print("[*] Starting Query #1.")
    elapsed_time = -time.time()
    spark_data = country_day_weeks_new_case_increment_with_new_cases_by_month(rdd, month,
                                                                              convert_to_obj,
                                                                              date_format)
    elapsed_time += time.time()
    print("[*] Completed, {:1.2f}s required.".format(np.round(elapsed_time, 0x2)))
    print("[*] Dumping in MongoDB.")
    MongoUtils.insert_many_to(MongoUtils.DB_NAME,
                              MongoUtils.COLLECTIONS_NAMES[0x1],
                              spark_data)
    print("[*] Done.")


def exec_country_recovered_wrt_new_case_until_date(rdd, country, date,
                                                   convert_to_obj=True,
                                                   date_format=__DATE_FORMAT__):
    print("[*] Starting Query #2.")
    elapsed_time = -time.time()
    spark_data = country_recovered_wrt_new_case_until_date(rdd, country, date,
                                                           convert_to_obj,
                                                           date_format)
    elapsed_time += time.time()
    print("[*] Completed, {:1.2f}s required.".format(np.round(elapsed_time, 0x2)))
    print("[*] Dumping in MongoDB.")
    MongoUtils.insert_many_to(MongoUtils.DB_NAME,
                              MongoUtils.COLLECTIONS_NAMES[0x2],
                              spark_data)
    print("[*] Done.")


if __name__ == '__main__':
    query_functions = [exec_country_new_cases_avg_by_month,
                       exec_country_day_weeks_new_case_increment_with_new_cases_by_month,
                       exec_country_recovered_wrt_new_case_until_date]

    params = {}

    parser = OptionParser()
    parser.add_option("-q", "--query", dest="query_id", help="Select Query.")
    parser.add_option("-m", "--month", dest="month", help="Set month for query 0 and 1")
    parser.add_option("-c", "--country", dest="country", help="Set country for query 2")
    parser.add_option("-d", "--date", dest="date", help="Set date for query 2")
    parser.add_option("-S", "--show_dataset_details", dest="show_dataset_details", action="store_true")

    (options, _) = parser.parse_args()

    query_id = int(options.query_id) if options.query_id is not None else -0x1

    if 0x0 <= query_id <= 0x1:
        month = int(options.month) if options.month is not None and options.month.isnumeric() else -0x1
        if month < 0x1 or month > 0xc:
            sys.stderr.write("Invalid param.\nQueryTool.py -h for help\n")
            exit(0x20)
        if query_id == 0x0:
            params = {"month": month}
        else:
            params = {"month": f"2020-{month}-01", "convert_to_obj": True}

    elif query_id == 0x2:
        country = options.country.lower() if options.country is not None else ""
        date = options.date if options.date is not None else ""

        if len(country) < 0x2:
            sys.stderr.write("Invalid param.\nQueryTool.py -h for help\n")
            exit(0x21)
        try:
            date = datetime.strptime(date, __DATE_FORMAT__).date()
        except Exception:
            sys.stderr.write("Invalid param.\nQueryTool.py -h for help\n")
            exit(0x22)

        params = {"country": country, "date": date, "convert_to_obj": False}

    else:
        sys.stderr.write("QueryTool.py -h for help\n")
        exit(0x10)

    print("[*] Initializing MongoDB.")
    MongoUtils.drop_db(MongoUtils.DB_NAME)
    MongoUtils.create_db(MongoUtils.DB_NAME)

    print("[*] Loading Data.")
    spark = init_spark()
    pd_dataframe = pd.read_csv(__DATASET_PATH__)

    spark_dataframe = spark.createDataFrame(pd_dataframe)
    dataframe_rdd = spark_dataframe.rdd

    params["rdd"] = dataframe_rdd

    if options.show_dataset_details:
        print("[*] Printing Dateset Details.")
        spark_dataframe.printSchema()
        spark_dataframe.show()

    query_functions[query_id](**params)
