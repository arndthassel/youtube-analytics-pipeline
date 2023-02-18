import pandas as pd
import datetime
import numpy as np


def subs_first_month(date_data, data):
    print('getting subs first month')
    date_data['date'] = pd.to_datetime(
        date_data['date'], format='%Y-%m-%d').dt.date

    min_date = pd.to_datetime(data['releasedOn'], format='%Y-%m-%d').dt.date
    max_date = min_date + datetime.timedelta(days=30)

    metric_list = []

    for i in data['#']:
        first_month_set = date_data[(date_data['videoNumber'] == i) & (
            date_data['date'] >= min_date[i-1]) & (date_data['date'] <= max_date[i-1])]
        subsum = first_month_set['subs'].sum()
        metric_list.append(subsum)

    data['subs first month'] = metric_list
    return (data['subs first month'])


def subs_permonth_from_month2(date_data, data):
    print('getting subs per month from month 2')
    date_data['date'] = pd.to_datetime(
        date_data['date'], format='%Y-%m-%d').dt.date

    min_date = pd.to_datetime(
        data['releasedOn'], format='%Y-%m-%d').dt.date + datetime.timedelta(days=30)
    max_date = max(date_data['date'])
    number_of_months = (max_date - min_date).dt.days / 30
    metric_list = []

    for i in data['#']:
        from_month2_set = date_data[(date_data['videoNumber'] == i) & (
            date_data['date'] >= min_date[i-1])]
        subsum = from_month2_set['subs'].sum()
        metric_list.append(subsum)

    data['subs from month 2'] = metric_list
    data['subs per month from month 2'] = data['subs from month 2'] / \
        np.where(number_of_months >= 1, number_of_months, np.NaN)
    return (data['subs per month from month 2'])


def organic_views_first_month(date_data, data):
    print('getting organic views first month')
    date_data['date'] = pd.to_datetime(
        date_data['date'], format='%Y-%m-%d').dt.date

    min_date = pd.to_datetime(data['releasedOn'], format='%Y-%m-%d').dt.date
    max_date = min_date + datetime.timedelta(days=30)

    metric_list = []

    for i in data['#']:
        first_month_set = date_data[(date_data['videoNumber'] == i) & (
            date_data['date'] >= min_date[i-1]) & (date_data['date'] <= max_date[i-1])]
        subsum = first_month_set['views'].sum(
        ) - first_month_set['V_ADVERTISING'].sum()
        metric_list.append(subsum)

    data['organic views first month'] = metric_list
    return (data['organic views first month'])


def organic_views_permonth_from_month2(date_data, data):
    print('getting organic views per month from month 2')
    date_data['date'] = pd.to_datetime(
        date_data['date'], format='%Y-%m-%d').dt.date

    min_date = pd.to_datetime(
        data['releasedOn'], format='%Y-%m-%d').dt.date + datetime.timedelta(days=30)
    max_date = max(date_data['date'])
    number_of_months = (max_date - min_date).dt.days / 30
    metric_list = []

    for i in data['#']:
        from_month2_set = date_data[(date_data['videoNumber'] == i) & (
            date_data['date'] >= min_date[i-1])]
        viewsum = from_month2_set['views'].sum(
        ) - from_month2_set['V_ADVERTISING'].sum()
        metric_list.append(viewsum)

    data['organic views from month 2'] = metric_list
    data['organic views per month from month 2'] = data['organic views from month 2'] / \
        np.where(number_of_months >= 1, number_of_months, np.NaN)
    return (data['organic views per month from month 2'])


def views_first_month_by_subscribers(date_data, data):
    print('getting views first month by subscribers')
    date_data['date'] = pd.to_datetime(
        date_data['date'], format='%Y-%m-%d').dt.date

    min_date = pd.to_datetime(data['releasedOn'], format='%Y-%m-%d').dt.date
    max_date = min_date + datetime.timedelta(days=30)

    metric_list = []

    for i in data['#']:
        first_month_set = date_data[(date_data['videoNumber'] == i) & (
            date_data['date'] >= min_date[i-1]) & (date_data['date'] <= max_date[i-1])]
        subsum = first_month_set['views by subscribers'].sum()
        metric_list.append(subsum)

    data['views first month by subscribers'] = metric_list
    return (data['views first month by subscribers'])
