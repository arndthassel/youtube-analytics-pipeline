from _0_auth import auth_a
import pandas as pd
import sys
sys.path.append('ADD YOUR PATH\\02_data_pipeline\\scripts')


def create_sheets():

    sheets = auth_a.get_auth_a_sheets()

    df = pd.read_csv(
        'ADD YOUR PATH\\02_data_pipeline\\data\\basic_with_sheetid.csv')
    df2 = pd.read_csv('ADD YOUR PATH\\02_data_pipeline\\data\\basic_data.csv')

    columns = df.columns.values.tolist()
    values = df.values.tolist()

    values2 = df2.values.tolist()

    # crete list of new videos
    newVideos = len(values2) - len(values)
    newList = []
    while newVideos > 0:
        newItem = values2[len(values2)-newVideos]
        newList.append(newItem)
        newVideos = newVideos-1
    print('new videos: ')
    print(newList)

    for item in newList:
        title = 'Video Data: #'+str(item[0])
        sheetProperties = {
            'properties': {
                'title': title
            }
        }
        sheet = sheets.spreadsheets().create(body=sheetProperties).execute()
        sid = sheet['spreadsheetId']
        item[5] = sid
        values.append(item)
        print(sid)
    result = pd.DataFrame(values, columns=columns)
    result.to_csv('ADD YOUR PATH\\02_data_pipeline\\data\\basic_with_sheetid.csv',
                  sep=',', encoding='utf-8', index=False)
    return (len(newList))
    # change sheet name to 'Main' and freeze view
