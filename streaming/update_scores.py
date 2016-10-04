from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row

from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

import json
from datetime import datetime

sc = SparkContext(appName="NFLPlays2")
ssc = StreamingContext(sc, 10)

kafkaStream = KafkaUtils.createStream(ssc, "ec2-52-55-25-69.compute-1.amazonaws.com:2181", "nfl_plays",  {"nfl_plays": 4})
lines = kafkaStream.map(lambda x: x[1])


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def make_ps_getpoints(table):
    #read points for player
    getpoints = "SELECT player_points FROM " + table + " WHERE player_id = ?"
    ps_getpoints = session.prepare(getpoints)
    #ps_getpoints.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return ps_getpoints

def make_ps_setpoints(table):
    #write new player points
    setpoints = "INSERT INTO " + table + " (player_id, player_name, position, player_points, playtime) VALUES (?,?,?,?,?)"
    ps_setpoints = session.prepare(setpoints)
    #ps_setpoints.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return ps_setpoints

def calculate_point_value(yards, touchdown):
    return (yards * 0.01 + touchdown * 0.01)

#connect to cassandra
cluster = Cluster(['ec2-52-44-4-126.compute-1.amazonaws.com'])
session = cluster.connect("nfl_plays")
ps_get_points = make_ps_getpoints("player_score")
ps_set_points = make_ps_setpoints("player_score")

def process(rdd):
    sqlContext = getSqlContextInstance(rdd.context)
    rowRDD = rdd.map(lambda w: Row(player_name=str(json.loads(w)["player_name"]), player_id=str(json.loads(w)["player_id"]),
                                   touchdown=json.loads(w)["touchdown"], yards=json.loads(w)["yards"],
                                   timestamp=json.loads(w)['timestamp'], position=json.loads(w)['position']))
    df_plays = sqlContext.createDataFrame(rowRDD)

    for row in df_plays.collect():
        playtime = row.timestamp
        if len(playtime) == 0:
            continue
        playtime = datetime.strptime(playtime, "%Y-%m-%d_%H:%M:%S")
        player_id = row.player_id
        player_name = row.player_name
        position = row.position
        yards = row.yards
        touchdown = row.yards
        # get current player points from DB
        curr_point_totals = session.execute(ps_get_points, (player_id,))
        try:
            curr_point_total = curr_point_totals[0].player_points
        except:
            curr_point_total = 0
        play_points = calculate_point_value(yards, touchdown)
        curr_point_total += play_points
        # put new player point total in DB
        session.execute(ps_set_points, (player_id, player_name, position, curr_point_total, playtime,))



lines.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
