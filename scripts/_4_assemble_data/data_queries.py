import pandas as pd
import sqlite3
import numpy as np

all_metrics = ['date', 'views', 'redViews', 'comments', 'likes', 'dislikes', 'videosAddedToPlaylists', 'videosRemovedFromPlaylists', 'shares', 'estimatedMinutesWatched', 'estimatedRedMinutesWatched', 'averageViewDuration', 'averageViewPercentage', 'annotationClickThroughRate', 'annotationCloseRate', 'annotationImpressions', 'annotationClickableImpressions', 'annotationClosableImpressions', 'annotationClicks', 'annotationCloses', 'cardClickRate', 'cardTeaserClickRate', 'cardImpressions', 'cardTeaserImpressions', 'cardClicks', 'cardTeaserClicks', 'subscribersGained', 'subscribersLost', 'estimatedRevenue', 'estimatedAdRevenue',
               'grossRevenue', 'estimatedRedPartnerRevenue', 'monetizedPlaybacks', 'playbackBasedCpm', 'adImpressions', 'cpm', 'MW_NO_LINK_OTHER', 'MW_ADVERTISING', 'MW_SUBSCRIBER', 'MW_YT_CHANNEL', 'MW_YT_SEARCH', 'MW_RELATED_VIDEO', 'MW_YT_OTHER_PAGE', 'MW_EXT_URL', 'MW_PLAYLIST', 'MW_NOTIFICATION', 'MW_YT_PLAYLIST_PAGE', 'MW_END_SCREEN', 'MW_ANNOTATION', 'MW_HASHTAGS', 'MW_SHORTS', 'V_NO_LINK_OTHER', 'V_ADVERTISING', 'V_SUBSCRIBER', 'V_YT_CHANNEL', 'V_YT_SEARCH', 'V_RELATED_VIDEO', 'V_YT_OTHER_PAGE', 'V_EXT_URL', 'V_PLAYLIST', 'V_NOTIFICATION', 'V_YT_PLAYLIST_PAGE', 'V_END_SCREEN', 'V_ANNOTATION', 'V_HASHTAGS', 'V_SHORTS']


def get_basic_data(connection):
    sql = """select videoNumber as '#', 
    sheetId as 'data_sheet',
    videoTitle as 'title', 
    duration as 'length (s)', 
    publishedAt as 'releasedOn' 
    from tblBasicData
    order by videoNumber"""
    df = pd.read_sql(sql=sql, con=connection)
    return (df)


def get_sum_metrics(connection):
    sql = """select sum(views) as views, 
    sum(estimatedMinutesWatched) as 'watchtime (min)', 
    sum(subscribersGained) as subs, sum(subscribersLost) subsLost, 
    sum(MW_ADVERTISING) as 'werbewatchtime (min)', 
    sum(estimatedMinutesWatched) - sum(MW_ADVERTISING) as 'organische Watchtime (min)',
    sum(V_ADVERTISING) as werbeviews,
    sum(views) - sum(V_ADVERTISING) as 'organische views',
    sum(annotationClicks) as annotationClicks,
    sum(MW_SUBSCRIBER) as 'watchtime subscriber',
    sum(V_SUBSCRIBER) as 'views subscriber',
    sum(V_YT_SEARCH) as 'views über YouTube Search'
    from tblDataWarehouse
    group by videoNumber
    order by videoNumber"""
    df = pd.read_sql(sql=sql, con=connection)
    return (df)


def get_date_metrics(connection):
    sql = """select videoNumber, 
    date,
    views,
    subscribersGained as subs,
    V_ADVERTISING,
    V_SUBSCRIBER as 'views by subscribers'
    from tblDataWarehouse
    order by videoNumber"""
    df = pd.read_sql(sql=sql, con=connection)
    return (df)


def get_audience_retention(connection):
    sql = """select videoNumber, 
    elapsedVideoTimeRatio,
    audienceWatchRatio
    from tblAudienceRetention"""
    df = pd.read_sql(sql=sql, con=connection)
    return (df)


def combine_dfs(connection):
    basic_data = get_basic_data(connection)
    metrics = get_sum_metrics(connection)
    data = pd.concat([basic_data, metrics], axis=1)
    return (data)
