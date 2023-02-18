from _0_auth import auth_a
import datetime

import requests
import isodate

import pandas as pd

import sys
sys.path.append('ADD YOUR PATH\\02_data_pipeline\\scripts')


def upload_view():

    sheets = auth_a.get_auth_a_sheets()

    overviewSheetId = 'INSERT YOUR SHEET ID'

    # 2 Get view data
    df = pd.read_csv('ADD YOUR PATH\\02_data_pipeline\\data\\view.csv')
    df.fillna('', inplace=True)
    # Convert to list format (for sheets API)
    columns = df.columns.values.tolist()
    values = df.values.tolist()

    values.reverse()

    basicVideoData = values
    videoNumber = len(basicVideoData)
    print('Data for '+str(videoNumber)+' videos found:')

    # 3 Write column names
    columnRange = 'Main!A5'
    columnNames = {'values': [columns]}
    response = sheets.spreadsheets().values().update(spreadsheetId=overviewSheetId,
                                                     range=columnRange, valueInputOption='USER_ENTERED', body=columnNames).execute()
    print(response)
    # 4 Append new videos
    updateRange = 'Main!A6'
    values = {'values': basicVideoData}
    response = sheets.spreadsheets().values().update(spreadsheetId=overviewSheetId,
                                                     range=updateRange, valueInputOption='USER_ENTERED', body=values).execute()
    print(response)

    # 5 update date
    newDate = datetime.date.today().strftime('%Y-%m-%d')
    dateRange = 'Main!A2'
    dateBody = {'values': [['Aktualisierungsdatum: '+newDate]]}

    response = sheets.spreadsheets().values().update(
        spreadsheetId=overviewSheetId,
        valueInputOption='USER_ENTERED',
        range=dateRange,
        body=dateBody
    ).execute()
