import pandas as pd
from _0_auth import auth_a
import time
import sys
sys.path.append('ADD YOUR PATH\\02_data_pipeline\\scripts')


sheets = auth_a.get_auth_a_sheets()

df = pd.read_csv(
    'ADD YOUR PATH\\02_data_pipeline\\data\\basic_with_sheetid.csv')

values = df.values.tolist()

for item in values:
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
        spreadsheetId=item[5],
        body=requestBody2
    ).execute()
    print('Sheet for Video '+str(item[0])+' adapted')
    time.sleep(0.5)
