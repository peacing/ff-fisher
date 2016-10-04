from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row

import json

sc = SparkContext(appName="NFLPlays")
ssc = StreamingContext(sc, 3)

kafkaStream = KafkaUtils.createStream(ssc, "ec2-52-55-25-69.compute-1.amazonaws.com:2181", "nfl_plays",  {"nfl_plays": 4})
lines = kafkaStream.map(lambda x: x[1])

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def process(rdd):
    sqlContext = getSqlContextInstance(rdd.context)
    rowRdd = rdd.map(
        lambda w: Row(player_name=str(json.loads(w)["player_name"]), player_id=str(json.loads(w)["player_id"]),
                      touchdown=json.loads(w)["touchdown"], yards=json.loads(w)["yards"],
                      timestamp=json.loads(w)['timestamp'], position=json.loads(w)['position']))

    df_plays = sqlContext.createDataFrame(rowRdd)
    df_plays.registerTempTable("plays")

    micro_player_points = sqlContext.sql("SELECT player_name, ( (SUM(yards) * .01) + (SUM(touchdown) * .01) ) as player_points from plays GROUP BY player_name")
    micro_player_points.registerTempTable("micro_player_points")

    users = sqlContext.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="micro_users", keyspace="nfl_plays") \
        .load()

    users.registerTempTable("users")

    user_micro_points = sqlContext.sql("SELECT u.user_id, SUM(p.player_points) as user_points FROM users u JOIN micro_player_points p ON u.player_name = p.player_name GROUP BY u.user_id ORDER BY user_points DESC")

    user_micro_points.registerTempTable("user_micro_points")

    top_scorer = user_micro_points.take(1)
    winner = top_scorer[0]
    winner_df = sqlContext.createDataFrame([winner])
    winner_df.registerTempTable("winner_df")

    winner_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode('append') \
    .options(table="micro_winners", keyspace="nfl_plays") \
    .save()

    winner_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode('overwrite') \
    .options(table="micro_winner", keyspace="nfl_plays") \
    .save()

windowed_lines = lines.window(30, 30)
windowed_lines.foreachRDD(process)
ssc.start()
ssc.awaitTermination()

