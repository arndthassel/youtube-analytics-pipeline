from _0_auth import auth_a
import time
import datetime

import requests
import isodate

import pandas as pd

import sys
sys.path.append('ADD YOUR PATH\\02_data_pipeline\\scripts')


def upload_video_data():
    sheets = auth_a.get_auth_a_sheets()

    df = pd.read_csv(
        'ADD YOUR PATH\\02_data_pipeline\\data\\basic_with_sheetid.csv')

    columns = df.columns.values.tolist()
    values = df.values.tolist()
    print(values)

    # Write each data for each individual video to individual Google Sheet
    for i in values:
        videoSheetId = i[5]
        videonumb = i[0]
        # update date
        newDate = datetime.date.today().strftime('%Y-%m-%d')
        dateRange = 'Main!G1'
        dateBody = {'values': [
            ['Aktualisierung:', newDate]], 'majorDimension': 'ROWS'}
        response = sheets.spreadsheets().values().update(
            spreadsheetId=videoSheetId,
            valueInputOption='USER_ENTERED',
            range=dateRange,
            body=dateBody
        ).execute()

        # Write Sheet Title in Cell A1
        a1Range = 'Main!A1'
        a1Body = {'values': [['Daten für Video '+str(i[0])+': '+str(
            i[2]), 'YouTube Analytics']], 'majorDimension': 'COLUMNS'}
        response = sheets.spreadsheets().values().update(
            spreadsheetId=videoSheetId,
            valueInputOption='USER_ENTERED',
            range=a1Range,
            body=a1Body
        ).execute()

        # create blank line
        values = [[]]
        n = 0
        while n < 38:
            values[0].append(' ')
            n = n+1

        starbody = {'values': values, 'majorDimension': 'ROWS'}
        starrange = 'Main!A3'

        response = sheets.spreadsheets().values().update(
            spreadsheetId=videoSheetId,
            valueInputOption='USER_ENTERED',
            range=starrange,
            body=starbody
        ).execute()

        # update values TS
        df2 = pd.read_csv(
            'ADD YOUR PATH\\02_data_pipeline\\data\\ts_by_video\\'+str(videonumb)+'.csv')
        columns = df2.columns.values.tolist()
        valueswithoutcolumns = df2.values.tolist()
        valueswithoutcolumns.reverse()
        del valueswithoutcolumns[0:3:]
        values = [columns]+valueswithoutcolumns

        valuesbody = {'values': values, 'majorDimension': 'ROWS'}
        valuesrange = 'Main!AK5'

        response = sheets.spreadsheets().values().update(
            spreadsheetId=videoSheetId,
            valueInputOption='USER_ENTERED',
            range=valuesrange,
            body=valuesbody
        ).execute()
        print('TS-Daten für Video'+str(i[0])+' erfolgreich aktualisiert')
        time.sleep(1)

        # update values main
        df3 = pd.read_csv(
            'ADD YOUR PATH\\02_data_pipeline\\data\\main_by_video\\'+str(videonumb)+'.csv')
        columns = df3.columns.values.tolist()
        valueswithoutcolumns = df3.values.tolist()
        valueswithoutcolumns.reverse()
        del valueswithoutcolumns[0:3:]
        values = [columns]+valueswithoutcolumns

        valuesbody = {'values': values, 'majorDimension': 'ROWS'}
        valuesrange = 'Main!A5'

        response = sheets.spreadsheets().values().update(
            spreadsheetId=videoSheetId,
            valueInputOption='USER_ENTERED',
            range=valuesrange,
            body=valuesbody
        ).execute()
        print('Main-Daten für Video'+str(i[0])+' erfolgreich aktualisiert')
        time.sleep(1)
