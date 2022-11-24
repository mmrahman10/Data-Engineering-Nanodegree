"""
#Iport os system function to operate on underlying Operating System tasks
#Import Python's built-in glob module to use the glob() functionwhich returns a list of files or folders that matches the path specified in the pathname argument
#Import PostgreSQL database driver and SQL create and drop table module to perform operations on PostgreSQL using python
#Import Pandas the Python library for data analysis
"""
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from typing import Any


def process_song_file(cur, filepath):
    """
    This function will processes the first part of ETL (ETL on the first dataset, song_data) 
    to create the songs and artists Dimensional Tables.
    
    Though this function will processes a single song file, 
    it will call from main to process all songs file in filepath one by one. 
    
    @param-1 cur: the database cursor
    @param-2 filepath: the path to the song file
    """
    # open song file
    df_song =  pd.read_json(filepath, typ='series')

    # insert song record
    song_data = df_song[["song_id", "title", "artist_id", "year", "duration"]] 
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =  df_song[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function will processes the second part of ETL (ETL on the second dataset, log_data)
    to create the time and users dimensional tables, as well as the songplays fact table.
    
    Though this function will processes a single log file, it will call from main to process 
    all songs file in filepath one by one 
    
    @param-1 cur: the database cursor
    @param-2 filepath: the path to the log file
    """
    # open log file
    df_log = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df_filter = df_log[df_log['page']=='NextSong']

    # convert timestamp column to datetime
    df_log["ts"] = pd.to_datetime(df_log["ts"], unit='ms')
    
    # insert time data records
    timestamps = df_log["ts"].dt.time
    hrs = df_log["ts"].dt.hour
    days = df_log["ts"].dt.day
    weeks = df_log["ts"].dt.week
    months = df_log["ts"].dt.month
    years = df_log["ts"].dt.year
    weekdays = df_log["ts"].dt.weekday
    column_labels = ("timestamp", "hour", "day", "week of year", "month", "year", "weekday")
    time_data = pd.DataFrame({"timestamp": timestamps, "hour": hrs, "day": days, "week": weeks, "month": months, "year": years, "weekday": weekdays})
    time_df =time_data 

    #Insert Records into Time Table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df_log[["userId", "firstName", "lastName", "gender", "level"]]
    #drop all duplicates rows and having emty and/or NAN values for usetId
    user_df = user_df.drop_duplicates().dropna()
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df_log.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        if row.userId != "":
            cur.execute(songplay_table_insert, songplay_data)
           


def process_data(cur, conn, filepath, func):
    """
    This function processes either log_files or songs depending on the given function.
    @param-1 cur: the database cursor
    @param-2 conn: the database connection
    @param-2 filepath: the path to the data directory
    @param-4 func: the function (process songs or logs)
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()