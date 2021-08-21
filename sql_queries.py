import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('song_dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songPlay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES
# ['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level', 'location', 'method', 'page', 'registration', 'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId']
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar(100),
        auth varchar(50),
        firstName varchar(100),
        gender varchar(1),
        itemInSession int,
        lastName varchar(100), 
        length decimal(7,5),
        level varchar(5), 
        location varchar(255),
        method varchar(5),
        page varchar(25), 
        registration varchar(100),
        sessionId int,
        song varchar(200),
        status varchar(5),
        ts timestamp,
        userAgent varchar(255),
        userId int
    );
""")

#['song_id', 'num_songs', 'title', 'artist_name', 'artist_latitude', 'year', 'duration', 'artist_id', 'artist_longitude', 'artist_location']
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id varchar(100),
        num_songs int,
        title varchar(200),
        artist_name varchar(100),
        artist_latitude decimal(8,6),
        year int,
        duration decimal(7,4),
        artist_id varchar(200),
        artist_longitude decimal(9,6),
        artist_location varchar(255)
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_songPlay (
        songplay_id IDENTITY(0,1) NOT NULL PRIMARY KEY,
        start_time timestamp REFERENCES dim_time (start_time),
        user_id int REFERENCES dim_user (user_id),
        level varchar(5),
        song_id varchar(100) REFERENCES dim_song (song_id),
        artist_id varchar(200) REFERENCES dim_artist (artist_id),
        session_id int,
        location varchar(255),
        user_agent varchar(255)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_user (
        user_id int NOT NULL PRIMARY KEY, 
        first_name varchar(100), 
        last_name varchar(100), 
        gender varchar(1), 
        level varchar(5)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_song (
        song_id varchar(100) NOT NULL PRIMARY KEY, 
        title varchar(200), 
        artist_id varchar(200), 
        year int, 
        duration decimal(7,4)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_artist (
        artist_id varchar(200) NOT NULL PRIMARY KEY, 
        name varchar(100), 
        location varchar(255), 
        lattitude decimal(8,6),
        longitude decimal(9,6)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_time (
        start_time timestamp PRIMARY KEY, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO fact_songPlay (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
    )
    SELECT DISTINCT
        ts as start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    FROM staging_events
    ;
""")

user_table_insert = ("""    
    INSERT INTO dim_user (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
    )
    SELECT DISTINCT
        user_id, 
        firstName as first_name, 
        lastName as last_name, 
        gender, 
        level
    FROM staging_events
    ;
""")

song_table_insert = ("""
    INSERT INTO dim_song (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
    )
    SELECT DISTINCT
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    FROM staging_songs
    ;
""")

artist_table_insert = ("""
    INSERT INTO dim_artist (
    artist_id, 
    name, 
    location, 
    lattitude, 
    longitude
    )
    SELECT DISTINCT
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_lattitude as lattitude,
        artist_longitude as longitude
    FROM staging_songs
    ;
""")

time_table_insert = ("""
    INSERT INTO dim_time (
    start_time, 
    hour, 
    day, 
    week,
    month,
    year,
    weekday
    )
    SELECT DISTINCT
        ts as start_time,
        EXTRACT('hour' from timestamp ts) AS hour,
        EXTRACT('day' from timestamp ts) AS day,
        EXTRACT('week' FROM timestamp ts) AS  week,
        EXTRACT('month' from timestamp ts) AS month,
        EXTRACT('year' from timestamp ts) AS year,
        EXTRACT(ISODOW from timestamp ts) AS weekday
    FROM staging_events
    ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
