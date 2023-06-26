import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

# STAGING TABLES: Prepare for ETL; for details see below at the staging phase.
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
);
"""
# The queries for user_table_copy, song_table_copy, artist_table_copy, and time_table_copy are not necessary during the staging phase before the ETL process.

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR NOT NULL
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration FLOAT
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
"""

# For the staging phase of ETL
# The purpose of the staging phase is to copy the raw data from the source files into the staging tables as-is, without any transformations or filtering. 
# Therefore, the only required COPY queries during the staging phase are staging_events_copy and staging_songs_copy, 
# which load the data from the source files into the staging tables staging_events and staging_songs, respectively.

# COPY QUERIES
staging_events_copy = """
COPY staging_events
FROM '{}'
IAM_ROLE '{}'
REGION 'us-west-2'
TIMEFORMAT AS 'epochmillisecs'
JSON '{}';
""".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = """
COPY staging_songs
FROM '{}'
IAM_ROLE '{}'
REGION 'us-west-2'
TIMEFORMAT AS 'epochmillisecs'
JSON 'auto';
""".format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# FINAL TABLES: After the ETL procs, the INSERT queries provided for user_table_copy, song_table_copy, artist_table_copy, and time_table_copy 
# are  necessary after the staging phase for ETL, to fill the final Fact+Dim. tables w/ ETL-processed content


# Dupe pruning: Since the AWS RS doesn't support the DISTINCT method, any naive queries will produce MANY duplicates... Hence:
# Based on the CTE method https://knowledge.udacity.com/questions/42129 using the ROW_NUMBER() function, we can improve the INSERT statements to remove duplicates
# e.g., in the queries below, the CTE uniq_staging_events selects the necessary fields from the staging_events table, 
# assigns a row number to each row within a user group (defined by PARTITION BY userid), and orders the rows in descending order based on the ts field. 
# The main query then selects the rows from the CTE where the rank is 1, which represents the latest snapshot of each user, and inserts them into the users table.

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
WITH uniq_staging_events AS (
    SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
                    e.userId AS user_id,
                    e.level,
                    s.song_id,
                    s.artist_id,
                    e.sessionId AS session_id,
                    e.location,
                    e.userAgent AS user_agent,
                    ROW_NUMBER() OVER (PARTITION BY e.userId, e.sessionId, s.song_id, s.artist_id ORDER BY e.ts DESC) AS rank
    FROM staging_events e
    JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name)
    WHERE e.page = 'NextSong'
)
SELECT start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
FROM uniq_staging_events
WHERE rank = 1
AND (
    SELECT COUNT(*)
    FROM songplays sp
    WHERE sp.start_time = uniq_staging_events.start_time
    AND sp.user_id = uniq_staging_events.user_id
) < 2;
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
WITH uniq_staging_events AS (
    SELECT userId AS user_id,
           firstName AS first_name,
           lastName AS last_name,
           gender,
           level,
           ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts DESC) AS rank
    FROM staging_events
    WHERE page = 'NextSong'
    AND user_id IS NOT NULL
)
SELECT user_id, first_name, last_name, gender, level
FROM uniq_staging_events
WHERE rank = 1;
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
WITH uniq_staging_songs AS (
    SELECT song_id,
           title,
           artist_id,
           year,
           duration,
           ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY song_id) AS rank
    FROM staging_songs
)
SELECT song_id, title, artist_id, year, duration
FROM uniq_staging_songs
WHERE rank = 1;
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
WITH uniq_staging_artists AS (
    SELECT artist_id,
           artist_name AS name,
           artist_location AS location,
           artist_latitude AS latitude,
           artist_longitude AS longitude,
           ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_id) AS rank
    FROM staging_songs
)
SELECT artist_id, name, location, latitude, longitude
FROM uniq_staging_artists
WHERE rank = 1;
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
WITH uniq_songplays AS (
    SELECT DISTINCT start_time,
                    EXTRACT(hour FROM start_time) AS hour,
                    EXTRACT(day FROM start_time) AS day,
                    EXTRACT(week FROM start_time) AS week,
                    EXTRACT(month FROM start_time) AS month,
                    EXTRACT(year FROM start_time) AS year,
                    EXTRACT(weekday FROM start_time) AS weekday,
                    ROW_NUMBER() OVER (PARTITION BY start_time ORDER BY start_time DESC) AS rank
    FROM songplays
)
SELECT start_time, hour, day, week, month, year, weekday
FROM uniq_songplays
WHERE rank = 1;
"""

# Query Lists
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]

