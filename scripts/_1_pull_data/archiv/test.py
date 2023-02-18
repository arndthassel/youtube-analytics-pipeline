from _0_auth import auth_a

import sys
sys.path.append('ADD YOUR PATH\\02_data_pipeline\\scripts')


sheets = auth_a.get_auth_a_sheets()
drive = auth_a.get_auth_a_drive()

# move files

sourceFolder = 'root'
ytanFolder = 'YOUR FOLDER ID'
videodataFolder = 'YOUR FOLDER ID'
response = drive.files().update(fileId=sid, addParents=videodataFolder,
                                removeParents=sourceFolder).execute()
print(response)

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
    ]
}


response2 = sheets.spreadsheets().batchUpdate(
    spreadsheetId='INSERT YOUR SHEET ID',
    body=requestBody2
).execute()

print(response2)
