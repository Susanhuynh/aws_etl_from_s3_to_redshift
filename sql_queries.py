import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS factSongPlay"
user_table_drop = "DROP TABLE IF EXISTS dimUser"
song_table_drop = "DROP TABLE IF EXISTS dimSong"
artist_table_drop = "DROP TABLE IF EXISTS dimArtist"
time_table_drop = "DROP TABLE IF EXISTS dimTime"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE "staging_songs" (
        num_songs          int,
        artist_id          varchar,
        artist_latitude    decimal,
        artist_longitude   decimal,
        artist_location    varchar,
        artist_name        varchar,
        song_id            varchar,
        title              varchar,
        duration           decimal,
        year               int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS "staging_events" (
        artist             varchar,
        auth               varchar,
        firstName          varchar,
        gender             varchar,
        itemInSession      int,
        lastName          varchar,
        length             decimal,
        level              varchar,
        location           varchar,
        method             varchar,
        page               varchar,
        registration       decimal,
        sessionId          varchar,
        song               varchar,
        status             int,
        ts                 bigint,
        userAgent          varchar,
        userId             varchar
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS "factSongPlay" (
        songplay_id        bigint identity(0,1) sortkey distkey,
        start_time         varchar,
        user_id            int,
        level              varchar,
        song_id            varchar,
        artist_id          varchar,
        session_id         int,
        location           varchar,
        user_agent         varchar
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS "dimUser" (
        user_id            int sortkey,
        first_name         varchar,
        last_name          varchar,
        gender             varchar,
        level              varchar
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS "dimSong" (
        song_id            varchar sortkey,
        title              varchar,
        artist_id          varchar,
        year               int,
        duration           decimal
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS "dimArtist" (
        artist_id         varchar sortkey,
        name              varchar,
        location          varchar,
        latitude          decimal,
        longitude         decimal
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS "dimTime" (
        start_time        timestamp sortkey,
        hour              int,
        day               int,
        week              int,
        month             int,
        year              int,
        weekday           int
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    format as JSON 'auto' 
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    format as JSON 'auto' 
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO dimUser (user_id, first_name, last_name, gender, level)
    (SELECT 
        DISTINCT CAST(userId as int),
        firstname,
        lastname,
        gender,
        level
    FROM staging_events 
    WHERE userId IS NOT NULL)
""")

user_table_insert = ("""
    INSERT INTO dimArtist (artist_id, name, location, latitude, longitude)
    (SELECT 
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
     FROM staging_songs
     WHERE artist_id IS NOT NULL)
""")

song_table_insert = ("""
    INSERT INTO dimSong (song_id, title, artist_id, year, duration)
    (SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
    )
""")

artist_table_insert = ("""
    INSERT INTO dimTime (start_time, hour, day, week, month, year, weekday)
    (SELECT
        st.start_time,
        EXTRACT(HOUR from st.start_time) as hour,
        EXTRACT(DAY from st.start_time) as day,
        EXTRACT(WEEK from st.start_time) as week,
        EXTRACT(MONTH from st.start_time) as month,
        EXTRACT(YEAR from st.start_time) as year,
        EXTRACT(WEEKDAY from st.start_time) as weekday 
    FROM 
        (SELECT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time
        FROM staging_events se) st
    )
""")

time_table_insert = ("""
    INSERT INTO factSongPlay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (SELECT
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
        cast(se.userId as int),
        se.level,
        ss.song_id,
        ss.artist_id,
        cast(se.sessionId as int),
        se.location,
        se.userAgent
    FROM staging_events se
    LEFT OUTER JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
    WHERE se.page = 'NextSong'
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
