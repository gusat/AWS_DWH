# SQL Query and ETL Pipeline

This repository contains Python scripts for executing SQL queries and running an ETL (Extract, Transform, Load) pipeline to populate a Redshift database.

## Files

- `sql_query.py`: Defines SQL queries for table creation, data loading, and data insertion.
- `create_tables.py`: Creates the required tables in the Redshift database using the queries from `sql_query.py`.
- `etl.py`: Loads data from S3 into staging tables and performs the ETL process to populate analytics tables.

## Prerequisites

Before running the scripts, make sure you have the following:

- Python environment with the required dependencies (`configparser`, `psycopg2`).
- `dwh.cfg` file: Contains the AWS Redshift cluster configuration, including the host, database name, user, password, and port, proper syntax (no quotes!)
- Access to an AWS Redshift cluster.

## Usage

Use the provided U_IaC_v1.1.ipynb (bonus automation) to create / pause / restart the AWS resources and RS cluster, w/o any need for AWS console.

Follow the steps below to run the SQL queries and execute the ETL pipeline:

1. Update the `dwh.cfg` file with your AWS Redshift cluster configuration.
2. Open a terminal or command prompt and navigate to the project directory.
3. Run `create_tables.py` to create the required tables in the Redshift database:

   ```bash
   python create_tables.py
   ```

4. Run `etl.py` to perform the ETL process:

   ```bash
   python etl.py
   ```

5. Monitor the output (validation code) for any errors or messages regarding table creation and data loading.


6. Sample analytics performed on a tiny subset

A) Baseline
SELECT COUNT(*) FROM songs; => 628
SELECT COUNT(*) FROM artists; => 611

B) What are the Top5 most played artists?
SELECT a.name AS artist_name, COUNT(*) AS play_count
FROM songplays sp
JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY a.name
ORDER BY play_count DESC
LIMIT 5;
=>
artist_name  play_count
Black Eyed Peas	3
The Rolling Stones	2
The Smiths	2
The Verve	2
Pearl Jam	1


C) What are the Top3 most played songs?
SELECT s.title AS song_title, a.name AS artist_name, COUNT(*) AS play_count
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY s.title, a.name
ORDER BY play_count DESC
LIMIT 1;
=>
song_title artist_name play_count
Let's Get It Started	        Black Eyed Peas	    3
Angie (1993 Digital Remaster)	The Rolling Stones	2
Bitter Sweet Symphony	        The Verve	        2


D) What's Sparkify's time histogram, i.e., busy_play_time?
SELECT hour, COUNT(*) AS play_count
FROM time t
JOIN songplays sp ON t.start_time = sp.start_time
GROUP BY hour
ORDER BY play_count DESC;
=>
hour 	play_count
15	3
21	3
17	3
18	2
8	1
19	1
12	1
22	1
14	1


E) Which locations have the highest number of song plays?
SELECT location, COUNT(*) AS play_count
FROM songplays
GROUP BY location
ORDER BY play_count DESC
LIMIT 10;
=>
location	                    play_count
San Francisco-Oakland-Hayward, CA	3
Tampa-St. Petersburg-Clearwater, FL	2
Portland-South Portland, ME	2
Atlanta-Sandy Springs-Roswell, GA	1
Chicago-Naperville-Elgin, IL-IN-WI	1
Janesville-Beloit, WI	1
Palestine, TX	1
Waterloo-Cedar Falls, IA	1
Detroit-Warren-Dearborn, MI	1
Red Bluff, CA	1


F) How many users are subscribed free vs. paid?
SELECT level, COUNT(DISTINCT user_id) AS user_count
FROM users
GROUP BY level;
=>
level user_count
paid	22
free	74

...

## License

This project is licensed under the [MIT License](LICENSE).

Feel free to modify and use the code according to your needs.

## Acknowledgments

- The SQL queries and ETL pipeline structure in this project are based on the Udacity Data Engineering Nanodegree program.