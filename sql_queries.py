import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE','ARN')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')

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
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id          integer IDENTITY(1,1),
        artist            varchar,
        auth              varchar,
        firstName         varchar,
        gender            char(1),
        itemInSession     integer,
        lastName          varchar,
        length            decimal,
        level             varchar,
        location          varchar,
        method            varchar,
        page              varchar,
        registration      bigint,
        sessionId         integer,
        song              varchar,
        status            integer,
        ts                timestamp,
        userAgent         varchar,
        userId            integer
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs          integer,
        artist_id          varchar,
        artist_latitude    DOUBLE PRECISION,
        artist_longitude   DOUBLE PRECISION,
        artist_location    varchar,
        artist_name        varchar,
        song_id            varchar,
        title              varchar,
        duration           decimal,
        year               integer             
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS factSongPlay (
        songplay_id     integer IDENTITY(1,1) PRIMARY KEY NOT NULL SORTKEY,
        start_time      timestamp NOT NULL REFERENCES dimTime(start_time), 
        user_id         integer NOT NULL DISTKEY REFERENCES dimUser(user_id), 
        level           varchar, 
        song_id         varchar REFERENCES dimSong(song_id), 
        artist_id       varchar REFERENCES dimArtist(artist_id), 
        session_id      integer, 
        location        varchar(300), 
        user_agent      varchar(300)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimUser (
        user_id            integer PRIMARY KEY NOT NULL SORTKEY DISTKEY, 
        first_name         varchar, 
        last_name          varchar, 
        gender             char(1), 
        level              varchar
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimSong (
        song_id            varchar PRIMARY KEY NOT NULL SORTKEY, 
        title              varchar NOT NULL, 
        artist_id          varchar, 
        year               integer, 
        duration           decimal NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimArtist (
        artist_id         varchar PRIMARY KEY NOT NULL SORTKEY, 
        name              varchar NOT NULL, 
        location          varchar, 
        latitude          DOUBLE PRECISION, 
        longitude         DOUBLE PRECISION
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimTime (
        start_time       timestamp PRIMARY KEY NOT NULL SORTKEY, 
        hour             integer, 
        day              integer, 
        week             integer, 
        month            integer, 
        year             integer, 
        weekday          integer
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    format as JSON {} 
    region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    format as JSON 'auto' 
    region 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO factSongPlay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
       se.ts,
       se.userId      as user_id,
       se.level       as level,
       ss.song_id     as song_id,
       ss.artist_id   as artist_id,
       se.sessionId   as session_id,
       se.location    as location,
       se.userAgent   as user_agent
    from staging_events se 
    join staging_songs ss
    on se.artist = ss.artist_name and page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO dimUser (user_id, first_name, last_name, gender, level)
    SELECT 
        DISTINCT userId as user_id,
        firstname as first_name,
        lastname as last_name,
        gender,
        level
    FROM staging_events 
    WHERE userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO dimSong (song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO dimArtist (artist_id, name, location, latitude, longitude)
    SELECT 
        DISTINCT artist_id as artist_id,
        artist_name as name, 
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
     FROM staging_songs
     WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    IINSERT INTO dimTime (start_time, hour, day, week, month, year, weekday)
    SELECT
       ts                         as start_time,
       EXTRACT(HOUR from ts)      as hour,
       EXTRACT(DAY  from ts)      as day,
       EXTRACT(WEEK from ts)      as week,
       EXTRACT(MONTH from ts)     as month,
       EXTRACT(YEAR from ts)      as year,
       EXTRACT(DAYOFWEEK from ts) as weekday
    FROM staging_events
    WHERE ts IS NOT NULL and page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, artist_table_create, time_table_create, song_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
