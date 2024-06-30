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
    "by_country": (
        [{"name": "ga:date"}, {"name": "ga:countryIsoCode"}],
        [
            {"expression": "ga:users"},
            {"expression": "ga:pageViews"},
            {"expression": "ga:sessions"},
            {"expression": "ga:sessionDuration"},
            {"expression": "ga:pageviewsPerSession"},
            {"expression": "ga:bounceRate"},
        ],
    ),
    "by_country_and_city": (
        [{"name": "ga:date"}, {"name": "ga:countryIsoCode"}, {"name": "ga:city"}],
        [
            {"expression": "ga:users"},
            {"expression": "ga:pageViews"},
            {"expression": "ga:sessions"},
            {"expression": "ga:sessionDuration"},
            {"expression": "ga:pageviewsPerSession"},
            {"expression": "ga:bounceRate"},
        ],
    ),
    "by_path": (
        [{"name": "ga:date"}, {"name": "ga:pagePath"}],
        [
            {"expression": "ga:users"},
            {"expression": "ga:pageViews"},
            {"expression": "ga:sessionDuration"},
            {"expression": "ga:pageviewsPerSession"},
            {"expression": "ga:bounceRate"},
        ],
    ),
    "by_source": (
        [{"name": "ga:date"}, {"name": "ga:source"}],
        [
            {"expression": "ga:users"},
            {"expression": "ga:pageViews"},
            {"expression": "ga:sessionDuration"},
            {"expression": "ga:pageviewsPerSession"},
            {"expression": "ga:bounceRate"},
        ],
    ),
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


def iterate_months(start, end):
    """Iterates over months in the supplied date range, returning tuples of dates
    representing each month's start and end.

    """
    if end < start:
        raise ValueError(f"End cannot be earlier than start: {end} < {start}.")

    def month_date_range(dt):
        _, mr_end = calendar.monthrange(dt.year, dt.month)
        mstart = max(start, date(dt.year, dt.month, 1))
        mend = min(end, date(dt.year, dt.month, mr_end))
        return mstart, mend

    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start, until=end):
        mstart, mend = month_date_range(dt)
        yield mstart, mend

    if mend < end:
        yield month_date_range(mend + timedelta(days=1))


def rm_f(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def dump_monthly(
    analytics, root: str, view_id: int, start_date: date, end_date: date, dims: list, metrics: list
):
    for mstart, mend in iterate_months(start_date, end_date):
        parent = f"{root}/{mstart.year:04}"
        os.makedirs(parent, exist_ok=True)

        csv_path = f"{parent}/{mstart.year:04}-{mstart.month:02}.csv"
        if os.path.exists(csv_path):
            print(f"{csv_path}: already exists")
            continue

        print(f"{csv_path}: fetching...", end="")
        response = get_report(analytics, view_id, mstart, mend, dims, metrics)
        print("done.")
        try:
            df = response_to_dataframe(response)
            df.to_csv(csv_path)
        except:
            rm_f(csv_path)
            raise


def dump(analytics, name: str, view_id: int, start_date: date, end_date: date):
    for dataset, (dims, metrics) in dims_metrics.items():
        root = f"data/{name}/{dataset}"
        print(f"starting dataset {root}")
        dump_monthly(analytics, root, view_id, start_date, end_date, dims, metrics)


analytics = initialize_analyticsreporting()
dump(analytics, "spack_docs", "153247856", date(2017, 6, 17), date(2022, 11, 12))
dump(analytics, "spack.io", "153231804", date(2017, 6, 17), date(2023, 7, 16))
dump(analytics, "spack_tutorial", "204724438", date(2019, 10, 27), date(2023, 7, 4))
dump(analytics, "eb_docs", "93323974", date(2016, 8, 23), date(2023, 1, 28))
