# DROP TABLES

songplay_table_drop = ("""DROP TABLE IF EXISTS songplays""")
user_table_drop = ("""DROP TABLE IF EXISTS users""")
song_table_drop = ("""DROP TABLE IF EXISTS songs""")
artist_table_drop = ("""DROP TABLE IF EXISTS artists""")
time_table_drop = ("""DROP TABLE IF EXISTS time""")

# CREATE TABLES

#    songplays is a fact table. Therefore, PK songplay_id is a serial which is just a sequence of records
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, start_time numeric NOT NULL, \
                            user_id int NOT NULL, level varchar NOT NULL, song_id varchar, artist_id varchar, session_id int NOT NULL, \
                            location varchar, user_agent varchar) ;""")

#    users table is a dimension table. Let user_id column be PK and prevent duplicate user_id values in this table.
user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar NOT NULL, last_name varchar NOT NULL, \
                        gender varchar NOT NULL, level varchar NOT NULL) ;""")

#    songs table is a dimension table. Let song_id column be PK and prevent duplicate song_id values in this table.
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar NOT NULL, \
                        artist_id varchar, year int, duration real)""")

#    artists table is a dimension table. Let artist_id column be PK and prevent duplicate artist_id values in this table.
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar NOT NULL, location varchar, \
                          latitude varchar, longitude varchar) ;""")

#    time table is a dimension table. Let start_time column be PK and prevent duplicate start_time values in this table.
time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time numeric PRIMARY KEY, hour int NOT NULL, day int NOT NULL, \
                        week int NOT NULL, month int NOT NULL, year int NOT NULL, weekday int NOT NULL) ;""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

#    For user table, ON CONFLICT(user_id), update level value
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (user_id) \
                        DO \
                            UPDATE \
                                SET level = EXCLUDED.level ;""")

#    ON CONFLICT (song_id) DO NOTHING prevent duplicates
song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (song_id) \
                        DO NOTHING ;""")

#    ON CONFLICT (artist_id) DO NOTHING prevent duplicates
artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) \
                          ON CONFLICT (artist_id) \
                          DO NOTHING ;""")

#    ON CONFLICT (start_time) DO NOTHING prevent duplicates
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) \
                        ON CONFLICT (start_time) \
                        DO NOTHING ;""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, songs.artist_id FROM (songs JOIN artists ON songs.artist_id=artists.artist_id) \
                  WHERE songs.title=%s AND artists.name=%s AND songs.duration=%s""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
