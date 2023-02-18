from _0_auth import auth_tb
import os
import pickle
import json
import pandas as pd
from pandas import json_normalize

import datetime

import sys
sys.path.append('ADD YOUR PATH\\02_data_pipeline\\scripts')


# load basic video data and create video dict
df = pd.read_csv('02_data_pipeline\data\\basic_data.csv')
videos = df.values.tolist()

youtubeAnalytics = auth_tb.get_auth_tb()
# Query


""" for i in videos:
    videonumb=i[0] """
video = 'video==b-ELQ84wUlE'  # +i[1]
response = youtubeAnalytics.reports().query(
    ids='INSERT CHANNEL ID HERE',
    ########### QUERY CODE############
    # i[3][0:10:], #Erstes Video des Kanals am 19.12.2017
    startDate='2017-12-19',
    endDate=datetime.date.today().strftime('%Y-%m-%d'),
    metrics='viewerPercentage',
    # ageGroup,subscribedStatusDimensionen, nach denen die Daten aufgeteilt werden; Beispiele: day, insightTrafficSourceType; Reihenfolge der Nennung ist relevant
    dimensions='gender,ageGroup,subscribedStatus'
    # filters=video, #Filter, welche die Gesamtmenge der ausgelesenen Daten reduzieren; VideoID "früher in Rente": ngdRIhdkmY0
    # sort='day' #Sortierung
).execute()

print(response)

# Verarbeitung des Query
data = response['rows']

columns = ['gender', 'ageGroup', 'subscribedStatus', 'viewerPercentage']

df = pd.DataFrame(data, columns=columns)

pathstring = 'ADD YOUR PATH\\02_data_pipeline\\data\\viewer_percentage\\channel.csv'
df.to_csv(pathstring, sep=',', encoding='utf-8', index=False)
print('ViewerPercentage for channel' + ' successfully saved')

""" pathstring='ADD YOUR PATH\\02_data_pipeline\\data\\viewer_percentage\\'+str(videonumb)+'.csv'
df.to_csv(pathstring, sep=',', encoding='utf-8', index=False)
print('ViewerPercentage for video '+str(videonumb)+' successfully saved') """
