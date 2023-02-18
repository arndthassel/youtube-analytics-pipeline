import json
import pandas as pd
import requests
import isodate


def pull_basic_data():
    print('Getting basic data...')
    # 1a get id of upload playlist containing all channel videos

    # 1 Get playlistitemdata (YT Data API) --> all videos by videoID, title and publication time
    videoData = []
    nextPageToken = ''
    nextPageTokenString = ''
    it = 0
    while it == 0 or (nextPageToken != ''):
        requestString = 'https://www.googleapis.com/youtube/v3/playlistItems?playlistId=UUDWg0IfUbB7uXZIgknuc6Ww&key=AIzaSyB1X9AJWx7z-LFXWXq0-sp-vdd16vXDVC4&part=snippet&maxResults=50'+nextPageTokenString
        it = it+1
        r = requests.get(requestString)
        data = r.json()
        if 'nextPageToken' in data:
            nextPageToken = data['nextPageToken']
            nextPageTokenStrin = '&pageToken='+nextPageToken
        else:
            nextPageToken = ''
        num = 1
        for item in data['items']:
            videoId = item['snippet']['resourceId']['videoId']
            videoTitle = item['snippet']['title']
            datePublished = item['snippet']['publishedAt']
            videoData.append([videoId, videoTitle, datePublished])
            # print(videoDict)

    # 2 get videos contentdetails data (YT Data API)
    it = 0
    for item in videoData:
        requestString = 'https://www.googleapis.com/youtube/v3/videos?id=' + \
            item[0]+'&part=contentDetails&key=AIzaSyB1X9AJWx7z-LFXWXq0-sp-vdd16vXDVC4'
        r = requests.get(requestString)
        data = r.json()
        dur = data['items'][0]['contentDetails']['duration']
        d = isodate.parse_duration(dur)
        videoData[it].append(d.total_seconds())
        it = it+1

    videoData.reverse()

    # Add videoNumber and videoId slot
    n = 1
    for item in videoData:
        item.insert(0, n)
        n = n+1
        item.append('')

    # add empty string value to insert link later

    df = pd.DataFrame(videoData, columns=[
                      'videoNumber', 'videoId', 'videoTitle', 'timePublished', 'videoLength', 'videoId'])
    df.to_csv('ADD YOUR PATH\\02_data_pipeline\data\\basic_data.csv',
              sep=',', encoding='utf-8', index=False)
    print('done')
