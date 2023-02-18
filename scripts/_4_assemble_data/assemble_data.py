import pandas as pd
import sqlite3
import datetime
import numpy as np
from _4_assemble_data import data_queries
from _4_assemble_data import sum_metrics
from _4_assemble_data import date_metrics
from _4_assemble_data import audience_retention_metrics


def assemble_data():
    path = 'ADD YOUR PATH\\02_data_pipeline\\data\\db.sqlite'
    connection = sqlite3.connect(path)

    # get data from SQL db
    data = data_queries.combine_dfs(connection)

    date_data = data_queries.get_date_metrics(connection)

    ar_data = data_queries.get_audience_retention(connection)

    # add new metrics to dataframe
    # SUM METRICS
    data['data_sheet'] = 'https://docs.google.com/spreadsheets/d/' + \
        data['data_sheet']

    data['daysSincePub'] = sum_metrics.days_since_pub(data)

    data['subscriber organisch'] = sum_metrics.subscribers_organic(data)

    data['watchtime per view'] = sum_metrics.watchtime_per_view(data)

    data['werbewatchtime per werbeview'] = sum_metrics.werbewatchtime_per_werbeview(
        data)

    data['organ watchtime per organ views'] = sum_metrics.organwatchtime_per_organviews(
        data)

    data['relative watchtime'] = sum_metrics.relative_watchtime(data)

    data['average watchtime organisch - werbe'] = data['organ watchtime per organ views'] - \
        data['werbewatchtime per werbeview']

    # DATE METRICS

    data['subs first month'] = date_metrics.subs_first_month(date_data, data)

    data['subs per month from month 2'] = date_metrics.subs_permonth_from_month2(
        date_data, data)

    data['organic views first month'] = date_metrics.organic_views_first_month(
        date_data, data)

    data['organic views per month from month 2'] = date_metrics.organic_views_permonth_from_month2(
        date_data, data)

    data['views first month by subscribers'] = date_metrics.views_first_month_by_subscribers(
        date_data, data)

    # AUDIENCE RETENTION

    data['retention after 60s'] = audience_retention_metrics.retention_after_60_sec(
        data, ar_data)

    pathstring = 'ADD YOUR PATH\\02_data_pipeline\\data\\view.csv'
    data.to_csv(pathstring, sep=',', encoding='utf-8', index=False)
    print('View saved')
