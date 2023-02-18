import os
import sqlite3
import pandas as pd
from _3_write_to_db import subroutines


def create_db():
    path = 'ADD YOUR PATH\\02_data_pipeline\\data\\db.sqlite'
    os.remove(path)

    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    subroutines.create_tblBasicData(connection)

    # get videonumber for loop
    cursor.execute('SELECT COUNT(*) from tblBasicData')
    result = cursor.fetchone()
    videonum = result[0]

    subroutines.create_tblDataWarehouse(connection, videonum)

    subroutines.create_tblAudienceRetention(connection, videonum)

    print('DB created')
