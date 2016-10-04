CREATE KEYSPACE nfl_plays WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
USE nfl_plays;

CREATE TABLE micro_winner (user_id text, user_points double, PRIMARY KEY (user_id));

CREATE TABLE user_players (user_id bigint, player_name text, PRIMARY KEY (user_id));

CREATE TABLE micro_winners (user_id text, user_points double, PRIMARY KEY (user_id));

CREATE TABLE raw_plays (timestamp text, player_id text, player_name text, position text, yards int, touchdown int, PRIMARY KEY (player_id));