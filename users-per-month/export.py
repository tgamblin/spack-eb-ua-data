#!/usr/bin/env python3

import calendar
from datetime import date
from datetime import timedelta
import pandas as pd
import json
import os
import pprint

from dateutil import rrule
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# Configuration
SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
KEY_FILE = "/Users/gamblin2/.gcp-creds/ga3-bigquery-426418-2d4247ec24b2.json"

# data sets we want to save for each data set
dims_metrics = {
    "users_per_month": (
        [{"name": "ga:year"}, {"name": "ga:month"}, {"name": "ga:countryIsoCode"}],
        [{"expression": "ga:users"}],
    )
}


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE, SCOPES)
    analytics = build("analyticsreporting", "v4", credentials=credentials)
    return analytics


def response_to_dataframe(response):
    list_rows = []
    for report in response.get("reports", []):
        columnHeader = report.get("columnHeader", {})
        dimensionHeaders = columnHeader.get("dimensions", [])
        metricHeaders = columnHeader.get("metricHeader", {}).get("metricHeaderEntries", [])

        for row in report.get("data", {}).get("rows", []):
            dimensions = row.get("dimensions", [])
            dateRangeValues = row.get("metrics", [])

            row_data = {}
            for header, dimension in zip(dimensionHeaders, dimensions):
                row_data[header] = dimension

            for values in dateRangeValues:
                for metricHeader, value in zip(metricHeaders, values.get("values")):
                    row_data[metricHeader.get("name")] = value

            list_rows.append(row_data)

    return pd.DataFrame(list_rows)


def get_report(analytics, view_id, start: date, end: date, dims: list, metrics: list):
    req = {
        "viewId": str(view_id),
        "dateRanges": [{"startDate": str(start), "endDate": str(end)}],
        "dimensions": dims,
        "metrics": metrics,
        "pageSize": 100000,
    }
    return analytics.reports().batchGet(body={"reportRequests": [req]}).execute()


def rm_f(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def dump(analytics, name: str, view_id: int, start_date: date, end_date: date):
    for dataset, (dims, metrics) in dims_metrics.items():
        root = f"data/{name}"
        os.makedirs(root, exist_ok=True)
        csv_path = f"{root}/{dataset}.csv"
        print(f"starting dataset {csv_path}")
        response = get_report(analytics, view_id, start_date, end_date, dims, metrics)
        print("done.")
        try:
            df = response_to_dataframe(response)
            df.to_csv(csv_path)
        except:
            rm_f(csv_path)
            raise


analytics = initialize_analyticsreporting()
dump(analytics, "spack_docs", "153247856", date(2017, 6, 17), date(2022, 11, 12))
# dump(analytics, "spack_ga4", "342268566", date(2022, 11, 12), date(2024, 6, 30))

dump(analytics, "eb_docs", "93323974", date(2016, 8, 23), date(2023, 1, 28))
# dump(analytics, "eb_ga4", "338601877", date(2023, 1, 28), date(2024, 6, 30))
