CREATE KEYSPACE nfl_plays WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
USE nfl_plays;

CREATE TABLE micro_winner2 (user_id text, user_points double, timestamp text, PRIMARY KEY (user_id));

CREATE TABLE user_players (user_id bigint, player_name text, PRIMARY KEY (user_id));

CREATE TABLE micro_winners3 (user_id text, user_points double, timestamp text, PRIMARY KEY (user_id, timestamp));

CREATE TABLE raw_plays (timestamp text, player_id text, player_name text, position text, yards int, touchdown int, PRIMARY KEY (player_id));

CREATE TABLE player_scores (player_name text, player_id text, position text, yards int, touchdown int, player_points double, playtime timestamp, PRIMARY KEY (player_name, playtime)) WITH CLUSTERING ORDER BY (playtime DESC);