# DROP TABLE QUERIES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLE QUERIES

#1. Create Fact Table songplays
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, \
start_time timestamp NOT NULL, user_id int NOT NULL, level varchar, song_id varchar, artist_id varchar, \
session_id int, location varchar, user_agent varchar);""")

#2. Create Dimension Tables 
#1. Create Table users
user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar, \
last_name varchar, gender varchar, level varchar NOT NULL);""")

#2. Create Table songs
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar NOT NULL, \
artist_id varchar, year int, duration decimal NOT NULL)""")

#3. Create Table artists
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar NOT NULL, \
location varchar, latitude double precision, longitude double precision);""")

#4. Create Table time
time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time time PRIMARY KEY, hour int, day int, week int, \
month int, year int, weekday int);""")

# INSERT RECORDS QUERIES
#1. Insert Data into songplays
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, \
location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (songplay_id) DO NOTHING""")

#2. Insert Data into users
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) \
ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level""")

#3. Insert Data into songs
song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) \
ON CONFLICT (song_id) DO NOTHING""")

#4. Insert Data into artists
artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) \
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING""")

#5. Insert Data into time from timestams of records 
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING""")

# FIND SONGS

song_select = ("""SELECT song_id, songs.artist_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id \
WHERE title = %s AND artists.name = %s AND songs.duration = %s""")

# QUERY LISTS
#create table query list to access in create_table.py file
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

#drop table list to access in create_table.py file
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
