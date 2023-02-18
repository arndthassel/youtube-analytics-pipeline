import pandas as pd
import datetime

# SUM METRICS


def days_since_pub(data):

    # trim releasedOn string and convert to date
    data['releasedOn'] = data['releasedOn'].str[:10]
    data['releasedOn'] = pd.to_datetime(
        data['releasedOn'], format='%Y-%m-%d').dt.date

    # add days since pub
    newDate = datetime.date.today() - datetime.timedelta(days=2)
    data['daysSincePub'] = newDate - data['releasedOn']
    return (data['daysSincePub'].dt.days)


def subscribers_organic(data):
    data['subscriber organisch'] = data['organische Watchtime (min)'] / \
        data['watchtime (min)']*data['subs']
    return (data['subscriber organisch'])


def watchtime_per_view(data):
    data['watchtime per view'] = data['watchtime (min)'] / data['views']
    return (data['watchtime per view'])


def werbewatchtime_per_werbeview(data):
    data['werbewatchtime per werbeview'] = data['werbewatchtime (min)'] / \
        data['werbeviews']
    return (data['werbewatchtime per werbeview'])


def organwatchtime_per_organviews(data):
    data['organ watchtime per organ views'] = data[
        'organische Watchtime (min)'] / data['organische views']
    return (data['organ watchtime per organ views'])


def relative_watchtime(data):
    data['relative watchtime'] = data['watchtime (min)'] / \
        data['views']*60/data['length (s)']
    return (data['relative watchtime'])
