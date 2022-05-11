import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")
ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"
user_table_drop = "DROP TABLE IF EXISTS users"


staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs
  (
     num_songs       INTEGER NOT NULL,
     artist_id       VARCHAR NOT NULL,
     artist_latitude DECIMAL,
     artist_location DECIMAL,
     artist_name     VARCHAR NOT NULL,
     song_id         VARCHAR NOT NULL,
     title           VARCHAR NOT NULL,
     duration        DECIMAL NOT NULL,
     year            INTEGER
  )
"""

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events
  (
     artist        VARCHAR NOT NULL,
     auth          VARCHAR NOT NULL,
     firtsname     VARCHAR NOT NULL,
     gender        VARCHAR,
     iteminsession INTEGER NOT NULL,
     lastname      VARCHAR,
     length        DECIMAL NOT NULL,
     level         VARCHAR,
     location      VARCHAR,
     method        VARCHAR NOT NULL,
     page          VARCHAR NOT NULL,
     registration  TIMESTAMP,
     sessionid     INTEGER NOT NULL,
     song          VARCHAR NOT NULL,
     status        INTEGER NOT NULL,
     ts            TIMESTAMP,
     useragent     VARCHAR,
     userid        INTEGER NOT NULL
  )
"""
songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplay
  (
     songplay_id INTEGER IDENTITY(0,1) sortkey distkey,
     start_time  DATE NOT NULL,
     user_id     INTEGER,
     level       VARCHAR,
     song_id     VARCHAR NOT NULL,
     artist_id   VARCHAR NOT NULL,
     session_id  VARCHAR NOT NULL,
     location    VARCHAR,
     user_agent  VARCHAR,
     PRIMARY KEY(songplay_id)
   )
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users

  (
     user_id    INTEGER sortkey,
     first_name VARCHAR,
     last_name  VARCHAR,
     gender     VARCHAR,
     level      VARCHAR,
     PRIMARY KEY(user_id)
  )
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS song
  (
     song_id   VARCHAR sortkey,
     title     VARCHAR NOT NULL,
     artist_id VARCHAR NOT NULL,
     year      INTEGER,
     duration  DECIMAL NOT NULL,
     PRIMARY KEY(song_id)
  )
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artist
  (
     artist_id VARCHAR sortkey,
     name      VARCHAR NOT NULL,
     location  VARCHAR,
     latitude  DECIMAL,
     longitude DECIMAL,
     PRIMARY KEY(artist_id)
  )
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time
  (
     start_time DATE sortkey,
     hour       INTEGER,
     day        INTEGER,
     week       INTEGER,
     month      INTEGER,
     year       INTEGER,
     weekday    INTEGER,
     PRIMARY KEY(start_time)
  )
"""

# STAGING TABLES

staging_events_copy = """
copy staging_events FROM '{}'
iam_role  '{}'
COMPUPDATE OFF region 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
format as json '{}'
""".format(
    LOG_DATA, ARN, LOG_JSONPATH
)


staging_songs_copy = """
copy staging_songs FROM '{}'
iam_role  '{}'
COMPUPDATE OFF region 'us-west-2'
FORMAT AS JSON 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""".format(
    SONG_DATA, ARN
)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays
            (start_time,
             user_id,
             level,
             song_id,
             artist_id,
             session_id,
             location,
             user_agent)
SELECT distinct ( se.ts ),
       se.userid,
       se.level,
       ss.song_id,
       ss.artist_id,
       se.sessionid,
       se.location,
       se.useragent
FROM   staging_events se
       JOIN staging_songs ss
         ON (se.title = ss.song AND se.artist = ss.artist_name)
WHERE  se.page = 'NextSong'
"""

user_table_insert = """
INSERT INTO users
            (user_id,
             first_name,
             last_name,
             gender,
             level)
SELECT DISTINCT( se.userid ),
               se.firstname,
               se.lastname,
               se.gender,
               se.level
FROM   staging_events se
"""

song_table_insert = """
INSERT INTO song
            (user_id,
             first_name,
             last_name,
             gender,
             level)
SELECT DISTINCT( se.userid ),
               se.firstname,
               se.lastname,
               se.gender,
               se.level
FROM   staging_events se

"""

artist_table_insert = """
INSERT INTO artist
            (artist_id,
             name,
             location,
             latitude,
             longitude)
SELECT DISTINCT( ss.artist_id ),
               ss.artist_name,
               ss.artist_location,
               ss.artist_latitude,
               ss.artist_longitude
FROM   staging_songs ss
"""

time_table_insert = """
INSERT INTO time
            (start_time,
             hour,
             day,
             week,
             month,
             year,
             weekday)
SELECT DISTINCT sp.start_time,
                Extract(hour FROM sp.start_time),
                Extract(day FROM sp.start_time),
                Extract(week FROM sp.start_time),
                Extract(month FROM sp.start_time),
                Extract(year FROM sp.start_time),
                Extract(dayofweek FROM sp.start_time)
FROM   songplay sp
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
