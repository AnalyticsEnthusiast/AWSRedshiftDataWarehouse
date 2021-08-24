import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('song_dwh.cfg')
ARN = config.get("ARN", "arn")
LOG_DATA = config.get("S3", "log_data")
SONG_DATA = config.get("S3", "song_data")
LOG_JSONPATH = config.get("S3", "log_jsonpath")
DWH_REGION = config.get("DWH", "dwh_region")


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songPlay CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS dim_user CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS dim_song CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS dim_time CASCADE;"

# CREATE TABLES
# ['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', 'level', 'location', 'method', 'page', 'registration', 'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId']
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar(200),
        auth varchar(50),
        firstName varchar(100),
        gender varchar(1),
        itemInSession int,
        lastName varchar(100), 
        length decimal(10,5),
        level varchar(5), 
        location varchar(255),
        method varchar(5),
        page varchar(25), 
        registration varchar(100),
        sessionId int,
        song varchar(200),
        status varchar(5),
        ts bigint,
        userAgent varchar(255),
        userId varchar(255)
    );
""")

#['song_id', 'num_songs', 'title', 'artist_name', 'artist_latitude', 'year', 'duration', 'artist_id', 'artist_longitude', 'artist_location']
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id varchar(100),
        num_songs int,
        title varchar(200),
        artist_name varchar(200),
        artist_latitude decimal(8,6),
        year int,
        duration decimal(9,4),
        artist_id varchar(200),
        artist_longitude decimal(9,6),
        artist_location varchar(255)
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_songPlay (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
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
        duration decimal(9,4)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_artist (
        artist_id varchar(200) NOT NULL PRIMARY KEY, 
        name varchar(200), 
        location varchar(255), 
        latitude decimal(8,6),
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
    COPY staging_events 
    FROM {}
    IAM_ROLE '{}'
    FORMAT AS JSON {}
    REGION '{}'
""").format(LOG_DATA, ARN, LOG_JSONPATH, DWH_REGION)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    IAM_ROLE '{}'
    FORMAT AS JSON 'auto'
    REGION '{}'
""").format(SONG_DATA, ARN, DWH_REGION)

# FINAL TABLES
user_table_insert = ("""    
    INSERT INTO dim_user (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
    )
    SELECT DISTINCT
        CAST(sq.userId as int) as user_id, 
        sq.firstName as first_name, 
        sq.lastName as last_name, 
        sq.gender, 
        sq.level
    FROM 
    (
        SELECT 
        userId,
        firstName,
        lastName,
        gender,
        level,
        row_number() OVER(PARTITION BY userId ORDER BY ts DESC) AS recent_user_id
        FROM staging_events
    ) sq
    WHERE userId <> ' '
    AND sq.recent_user_id = 1
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
    latitude, 
    longitude
    )
    SELECT DISTINCT
        ss.artist_id,
        ss.artist_name as name,
        ss.artist_location as location,
        ss.artist_latitude as latitude,
        ss.artist_longitude as longitude
    FROM (
        SELECT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude,
        ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY duration DESC) as track_order
        FROM staging_songs
    ) ss
    WHERE ss.track_order = 1
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
        date_add('ms',ts,'1970-01-01') as start_time,
        EXTRACT('hour' from start_time) AS hour,
        EXTRACT('day' from start_time) AS day,
        EXTRACT('week' from start_time) AS  week,
        EXTRACT('month' from  start_time) AS month,
        EXTRACT('year' from start_time) AS year,
        EXTRACT('DOW' from start_time) AS weekday
    FROM staging_events
    ;
""")

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
    SELECT
        date_add('ms',se.ts,'1970-01-01') as start_time,
        CAST(se.userId as int) as user_id,
        se.level,
        ss.song_id as song_id,
        ss.artist_id as artist_id,
        se.sessionId as session_id,
        se.location,
        se.useragent as user_agent
    FROM staging_events se
    LEFT JOIN (
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            artist_name
        FROM staging_songs
    ) ss
    ON se.song = ss.title
    AND se.artist = ss.artist_name
    WHERE userId <> ' '
    ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
