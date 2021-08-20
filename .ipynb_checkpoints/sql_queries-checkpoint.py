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

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    
    
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    
    
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_songPlay (
    
    
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_user (
    
    
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_song (
    
    
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_artist (
    
    
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_time (
    
    
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
    
    
    
    )
    VALUES ();
""")

user_table_insert = ("""    
    INSERT INTO dim_user (
    
    
    
    )
    VALUES ();
""")

song_table_insert = ("""
    INSERT INTO dim_song (
    
    
    
    )
    VALUES ();
""")

artist_table_insert = ("""
    INSERT INTO dim_artist (
    
    
    
    )
    VALUES ();
""")

time_table_insert = ("""
    INSERT INTO dim_time (
    
    
    
    )
    VALUES ();
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
