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
KEY_FILE = "/Users/gamblin2/creds/ga3-bigquery-426418-2d4247ec24b2.json"


common_dims = [{"name": "ga:date"}]
no_nday_users_dims = [
    {"name": "ga:countryIsoCode"},
    {"name": "ga:city"},
    {"name": "ga:operatingSystem"},
    {"name": "ga:fullReferrer"},
    {"name": "ga:pagePath"},
    {"name": "ga:language"},
]

common_metrics = [
    {"expression": "ga:sessions"},
    {"expression": "ga:pageviews"},
    {"expression": "ga:newUsers"},
    {"expression": "ga:sessionDuration"},
    {"expression": "ga:avgSessionDuration"},
    {"expression": "ga:pageviewsPerSession"},
    {"expression": "ga:bounceRate"},
    {"expression": "ga:organicSearches"},
]
no_nday_users_metrics = [{"expression": "ga:sessionsPerUser"}]


dims_metrics = {
    "users": (
        common_dims + no_nday_users_dims,
        [{"expression": "ga:users"}] + common_metrics + no_nday_users_metrics,
    ),
    #    "1dayUsers": (common_dims, [{"expression": "ga:1dayUsers"}] + common_metrics),
    #    "7dayUsers": (common_dims, [{"expression": "ga:7dayUsers"}] + common_metrics),
    #    "30dayUsers": (common_dims, [{"expression": "ga:30dayUsers"}] + common_metrics),
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
    pprint.pprint(req)

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
        print(dims)
        print()
        print(metrics)
        print()

        root = f"{name}/{dataset}"
        print(f"starting dataset {root}")
        dump_monthly(analytics, root, view_id, start_date, end_date, dims, metrics)


analytics = initialize_analyticsreporting()

# dump("spack_docs", "153247856", date(2017, 6, 17), date(2022, 11, 12))
dump(analytics, "spack_docs", "153247856", date(2021, 6, 1), date(2021, 6, 31))
