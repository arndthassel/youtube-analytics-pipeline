from _0_auth import auth_tb
import os
import pickle
import json
import pandas as pd
from pandas import json_normalize

import datetime

import sys
sys.path.append('ADD YOUR PATH\\02_data_pipeline\\scripts')


def pull_pervideo_main():

    # load basic video data and create video dict
    df = pd.read_csv('ADD YOUR PATH\\02_data_pipeline\data\\basic_data.csv')
    videos = df.values.tolist()

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
    metrics = ['views', 'redViews', 'comments', 'likes', 'dislikes', 'videosAddedToPlaylists', 'videosRemovedFromPlaylists', 'shares', 'estimatedMinutesWatched', 'estimatedRedMinutesWatched', 'averageViewDuration', 'averageViewPercentage', 'annotationClickThroughRate', 'annotationCloseRate', 'annotationImpressions', 'annotationClickableImpressions', 'annotationClosableImpressions',
               'annotationClicks', 'annotationCloses', 'cardClickRate', 'cardTeaserClickRate', 'cardImpressions', 'cardTeaserImpressions', 'cardClicks', 'cardTeaserClicks', 'subscribersGained', 'subscribersLost', 'estimatedRevenue', 'estimatedAdRevenue', 'grossRevenue', 'estimatedRedPartnerRevenue', 'monetizedPlaybacks', 'playbackBasedCpm', 'adImpressions', 'cpm']
    indexdict = {}
    n = 0
    for i in metrics:
        indexdict[i] = n
        n = n+1

    # create empty data

    for i in datelist:
        n = 1
        while n <= len(metrics):
            i.append(0)
            n = n+1

    youtubeAnalytics = auth_tb.get_auth_tb()
    # Query
    for i in videos:
        datelistrun = datelist
        for o in datelistrun:
            q = 1
            for p in o:
                while q <= len(metrics):
                    o[q] = 0
                    q = q+1

        videonumb = i[0]
        video = 'video=='+i[1]
        response = youtubeAnalytics.reports().query(
            ids='INSERT CHANNEL ID HERE',
            ########### QUERY CODE############
            startDate=i[3][0:10:],  # Erstes Video des Kanals am 19.12.2017
            endDate=datetime.date.today().strftime('%Y-%m-%d'),
            metrics='views,redViews,comments,likes,dislikes,videosAddedToPlaylists,videosRemovedFromPlaylists,shares,estimatedMinutesWatched,estimatedRedMinutesWatched,averageViewDuration,averageViewPercentage,annotationClickThroughRate,annotationCloseRate,annotationImpressions,annotationClickableImpressions,annotationClosableImpressions,annotationClicks,annotationCloses,cardClickRate,cardTeaserClickRate,cardImpressions,cardTeaserImpressions,cardClicks,cardTeaserClicks,subscribersGained,subscribersLost,estimatedRevenue,estimatedAdRevenue,grossRevenue,estimatedRedPartnerRevenue,monetizedPlaybacks,playbackBasedCpm,adImpressions,cpm',
            dimensions='day',  # Dimensionen, nach denen die Daten aufgeteilt werden; Beispiele: day, insightTrafficSourceType; Reihenfolge der Nennung ist relevant
            filters=video,  # Filter, welche die Gesamtmenge der ausgelesenen Daten reduzieren; VideoID "fr??her in Rente": ngdRIhdkmY0
            # sort='day' #Sortierung
        ).execute()

        # Verarbeitung des Query
        data = response['rows']

        for m in data:
            r = 1
            while r <= len(data[0])-1:
                datelistrun[datedict[m[0]]][r] = m[r]
                r = r+1

        columns = ['date']+metrics

        df = pd.DataFrame(datelistrun, columns=columns)

        pathstring = 'ADD YOUR PATH\\02_data_pipeline\\data\\main_by_video\\' + \
            str(videonumb)+'.csv'
        df.to_csv(pathstring, sep=',', encoding='utf-8', index=False)
        print('maindata for video '+str(videonumb)+' successfully saved')
