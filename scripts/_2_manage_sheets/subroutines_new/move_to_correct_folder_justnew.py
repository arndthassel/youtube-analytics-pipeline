from _0_auth import auth_a
import time
import pandas as pd
import sys
sys.path.append('ADD YOUR PATH\\02_data_pipeline\\scripts')


def move_to_correct_folder(new_videos):

    drive = auth_a.get_auth_a_drive()

    df = pd.read_csv(
        'ADD YOUR PATH\\02_data_pipeline\\data\\basic_with_sheetid.csv')

    values = df.values.tolist()

    # How many new videos?
    numb_of_new_videos = new_videos
    total_numb_of_videos = len(values)
    list_of_sheetids = []
    n = 0
    while n < numb_of_new_videos:
        list_of_sheetids.append(values[total_numb_of_videos-n-1][5])
        n = n+1

    sourceFolder = 'root'
    videodataFolder = 'YOUR FOLDER ID'

    for i in list_of_sheetids:
        response = drive.files().update(fileId=i, addParents=videodataFolder,
                                        removeParents=sourceFolder).execute()
        print(response)
        print('File for Video with SheetId ' + i + ' successfully moved')
        time.sleep(0.5)
