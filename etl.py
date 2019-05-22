import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Extract song file (JSON) from directory and insert record into song and artist tables

    Arguments:
        cur: DB cursor connection
        filepath: path to a file
    Return: None  
    """
    # open song file
    try:
        df = pd.read_json(filepath, lines=True)
    except Exception as e:
        print("Error: Cannot read song file ", filepath)
        print(e)
              
    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    try:
        cur.execute(song_table_insert, song_data)
    except Exception as e:
        print("Error: Cannot insert song data ", song_data)
        print(e)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    try:
        cur.execute(artist_table_insert, artist_data)
    except Exception as e:
        print("Error: Cannot insert artist data ", artist_data)
        print(e)


def process_log_file(cur, filepath):
    """
    Extract song play activity log file (JSON) from directory and insert records into time, user, and songplay tables
    For songplay, need to lookup song and artist table for songid and artistid
    
    Arguments:
        cur: DB cursor connection
        filepath: path to a file
    Return: None  
    """
    
    # open log file
    try:
        df = pd.read_json(filepath, lines=True)
    except Exception as e:
        print("Error: Cannot read log file ", filepath)
        print(e)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [df['ts'], t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame({column_labels[0]:time_data[0], column_labels[1]:time_data[1], column_labels[2]:time_data[2],
                            column_labels[3]:time_data[3], column_labels[4]:time_data[4], column_labels[5]:time_data[5], column_labels[6]:time_data[6]})

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except Exception as e:
            print("Error: Cannot insert time data ", list(row))
            print(e)            

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except Exception as e:
            print("Error: Cannot insert user data ", list(row))
            print(e)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except Exception as e:
            print("Error: Cannot insert songplay data ", songplay_data)
            print(e)

def process_data(cur, conn, filepath, func):
    """
    Find all files (JSON format) in the filepath and run the func for each file
    This process eventually process file data and saves into database tables
    
    Arguments:
        cur: DB cursor connection
        conn: DB connection to Postgres
        filepath: path to files
        func: function to be called for processing each file
    Return: None  
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
        try:
            func(cur, datafile)
            print('{}/{} files processed.'.format(i, num_files))
        except Exception as e:
            print('Error: {}/{} files not processed.'.format(i, num_files))
        finally:
            conn.commit()

def main():
    """
    Main function for processing Sparkify song and log files
    
    Arguments: None
    Return: None  
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    try:
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    except Exception as e:
        print("Error: while processing dataset files")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
