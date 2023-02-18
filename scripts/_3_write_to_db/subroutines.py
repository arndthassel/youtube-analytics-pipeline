import os
import sqlite3
import pandas as pd


def create_tblBasicData(connection):
    # create table for basic data
    df = pd.read_csv(
        'ADD YOUR PATH\\02_data_pipeline\\data\\basic_with_sheetid.csv')
    df.to_sql(name='tblBasicData', con=connection,
              if_exists='replace', index=False)
    sql_query = """select * from tblBasicData"""
    df = pd.read_sql(sql=sql_query, con=connection)


def create_tblDataWarehouse(connection, videonum):
    # create table for data warehouse
    n = 1
    while n <= videonum:
        file_main = 'ADD YOUR PATH\\02_data_pipeline\\data\\main_by_video\\' + \
            str(n) + '.csv'
        file_ts = 'ADD YOUR PATH\\02_data_pipeline\\data\\ts_by_video\\' + \
            str(n)+'.csv'
        df1 = pd.read_csv(file_main)
        df2 = pd.read_csv(file_ts)
        # create single dataframe with all data
        df3 = df1.merge(df2)
        # add videonumber to each row (needed as primary key)
        video_number_list = [n]*df2.shape[0]
        df3.insert(loc=0, column='videoNumber', value=video_number_list)
        # write to data warehouse
        df3.drop(df3.tail(3).index, inplace=True)
        df3.to_sql(name='tblDataWarehouse', con=connection,
                   if_exists='append', index=False)
        # delete last three rows because they are always 0
        print(f'Video {n} added to Data Warehouse')
        n = n + 1


def create_tblAudienceRetention(connection, videonum):

    # create table for audience retention
    n = 1
    while n <= videonum:
        csv_file = 'ADD YOUR PATH\\02_data_pipeline\\data\\audience_retention\\' + \
            str(n)+'.csv'
        df = pd.read_csv(csv_file)
        # add videonumber to each row (needed as primary key)
        video_number_list = [n]*df.shape[0]
        df.insert(loc=0, column='videoNumber', value=video_number_list)
        # write to
        df.to_sql(name='tblAudienceRetention', con=connection,
                  if_exists='append', index=False)
        print(f'Video {n} added to Audience Retention Table')
        n = n + 1
