from _0_auth import auth_tb
import os
import pickle
import json
import pandas as pd
from pandas import json_normalize

import datetime

import sys
sys.path.append('ADD YOUR PATH\\02_data_pipeline\\scripts')


def pull_channel_ts():
    # Create Datelist
    print('creating datelist')
    datelist = []
    initialdatestr = '2017-12-19'
    date = datetime.datetime.strptime(initialdatestr, '%Y-%m-%d').date()
    finaldate = datetime.date.today()
    while date <= finaldate:
        string = date.strftime('%Y-%m-%d')
        datelist.append([string])
        date = date+datetime.timedelta(days=1)

    # create datedict (connecting dates with their index)
    datedict = {}
    n = 0
    for i in datelist:
        datedict[i[0]] = n
        n = n+1

    # create indexdict (connecting trafficsourcetypes with their index)
    trafficSourceTypes = ['NO_LINK_OTHER', 'ADVERTISING', 'SUBSCRIBER', 'YT_CHANNEL', 'YT_SEARCH', 'RELATED_VIDEO',
                          'YT_OTHER_PAGE', 'EXT_URL', 'PLAYLIST', 'NOTIFICATION', 'YT_PLAYLIST_PAGE', 'END_SCREEN', 'ANNOTATION', 'HASHTAGS', 'SHORTS']
    indexdict = {}
    n = 0
    for i in trafficSourceTypes:
        indexdict[i] = n
        n = n+1

    # create empty data
    for i in datelist:
        n = 1
        while n <= 30:
            i.append(0)
            n = n+1

    TTcolumns = ['MW_NO_LINK_OTHER', 'MW_ADVERTISING', 'MW_SUBSCRIBER', 'MW_YT_CHANNEL', 'MW_YT_SEARCH', 'MW_RELATED_VIDEO', 'MW_YT_OTHER_PAGE', 'MW_EXT_URL', 'MW_PLAYLIST', 'MW_NOTIFICATION', 'MW_YT_PLAYLIST_PAGE', 'MW_END_SCREEN', 'MW_ANNOTATION', 'MW_HASHTAGS',
                 'MW_SHORTS', 'V_NO_LINK_OTHER', 'V_ADVERTISING', 'V_SUBSCRIBER', 'V_YT_CHANNEL', 'V_YT_SEARCH', 'V_RELATED_VIDEO', 'V_YT_OTHER_PAGE', 'V_EXT_URL', 'V_PLAYLIST', 'V_NOTIFICATION', 'V_YT_PLAYLIST_PAGE', 'V_END_SCREEN', 'V_ANNOTATION', 'V_HASHTAGS', 'V_SHORTS']

    metrics = ['views', 'redViews', 'comments', 'likes', 'dislikes', 'videosAddedToPlaylists', 'videosRemovedFromPlaylists', 'shares', 'estimatedMinutesWatched', 'estimatedRedMinutesWatched', 'averageViewDuration', 'averageViewPercentage', 'annotationClickThroughRate', 'annotationCloseRate', 'annotationImpressions', 'annotationClickableImpressions', 'annotationClosableImpressions',
               'annotationClicks', 'annotationCloses', 'cardClickRate', 'cardTeaserClickRate', 'cardImpressions', 'cardTeaserImpressions', 'cardClicks', 'cardTeaserClicks', 'subscribersGained', 'subscribersLost', 'uniques', 'estimatedRevenue', 'estimatedAdRevenue', 'grossRevenue', 'estimatedRedPartnerRevenue', 'monetizedPlaybacks', 'playbackBasedCpm', 'adImpressions', 'cpm']

    # Query

    datelistrun = datelist
    for o in datelistrun:
        q = 1
        for p in o:
            while q < 31:
                o[q] = 0
                q = q+1

    youtubeAnalytics = auth_tb.get_auth_tb()
    response = youtubeAnalytics.reports().query(
        ids='INSERT CHANNEL ID HERE',
        ########### QUERY CODE############
        startDate='2017-12-19',
        endDate=datetime.date.today().strftime('%Y-%m-%d'),
        metrics='estimatedMinutesWatched,views',  # Daten (Metriken), die ausgelesen werden können: views, estimatedMinutesWatched, shares, comments, likes, videosAddedToPlaylists, averageViewDuration, videosRemovedFromPlaylists, redViews, averageViewPercentage, cardTeaserClickRate, cardImpressions, cardTeaserImpressions, cardClicks, cardTeaserClicks, subscribersGained, subscribersLost, estimatedRevenue, estimatedAdRevenue, grossRevenue, estimatedRedPartnerRevenue, monetizedPlaybacks, playbackBasedCpm, adImpressions, cpm
        # Dimensionen, nach denen die Daten aufgeteilt werden; Beispiele: day, insightTrafficSourceType; Reihenfolge der Nennung ist relevant
        dimensions='day,insightTrafficSourceType',
        # filters=video, #Filter, welche die Gesamtmenge der ausgelesenen Daten reduzieren; VideoID "früher in Rente": ngdRIhdkmY0
        # sort='day' #Sortierung
    ).execute()

    # Verarbeitung des Query
    data = response['rows']
    for m in data:
        datelistrun[datedict[m[0]]][indexdict[m[1]]+1] = m[2]
        datelistrun[datedict[m[0]]][indexdict[m[1]]+16] = m[3]

    columns = ['date']+TTcolumns

    df = pd.DataFrame(datelistrun, columns=columns)

    pathstring = 'ADD YOUR PATH\\02_data_pipeline\\data\\channel_ts.csv'
    df.to_csv(pathstring, sep=',', encoding='utf-8', index=False)
    print('trafficsourscedata for channel successfully saved')
