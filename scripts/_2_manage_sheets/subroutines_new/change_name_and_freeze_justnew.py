import pandas as pd
from _0_auth import auth_a
import time
import sys
sys.path.append('ADD YOUR PATH\\02_data_pipeline\\scripts')


def change_name_and_freeze(new_videos):

    sheets = auth_a.get_auth_a_sheets()

    df = pd.read_csv(
        'ADD YOUR PATH\\02_data_pipeline\\data\\basic_with_sheetid.csv')

    values = df.values.tolist()

    # How many new videos?
    numb_of_new_videos = new_videos
    total_numb_of_videos = len(values)
    list_of_sheetids = []
    n = 0
    while n < numb_of_new_videos:
        list_of_sheetids.append(values[total_numb_of_videos - n - 1][5])
        n = n + 1

    print('SheetIds od new videos: ' + str(list_of_sheetids))

    for i in list_of_sheetids:
        requestBody2 = {
            'requests': [
                {
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': 0,
                            'title': 'Main',
                            'gridProperties': {
                                'frozenRowCount': 5,
                                'frozenColumnCount': 1,
                            },
                        },
                        'fields': 'title,gridProperties(frozenRowCount, frozenColumnCount)',
                    }
                }
            ]}

        response2 = sheets.spreadsheets().batchUpdate(
            spreadsheetId=i,
            body=requestBody2
        ).execute()
        print('Sheet for Video with SheetID ' + i + ' adapted')
        time.sleep(0.5)
