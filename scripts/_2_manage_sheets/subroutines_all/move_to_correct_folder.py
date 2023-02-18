from _0_auth import auth_a
import time
import pandas as pd
import sys
sys.path.append('ADD YOUR PATH\\02_data_pipeline\\scripts')

drive = auth_a.get_auth_a_drive()

df = pd.read_csv(
    'ADD YOUR PATH\\02_data_pipeline\\data\\basic_with_sheetid.csv')

values = df.values.tolist()

sourceFolder = 'root'
videodataFolder = 'YOUR FOLDER ID'
for item in values:
    response = drive.files().update(
        fileId=item[5], addParents=videodataFolder, removeParents=sourceFolder).execute()
    print(response)
    print('File for Video '+str(item[0])+' successfully moved')
    time.sleep(0.5)





