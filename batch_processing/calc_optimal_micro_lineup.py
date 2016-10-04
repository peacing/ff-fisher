from pyspark.sql import SQLContext
from pyspark import SparkContext

import datetime
from dateutil import tz
import time

FIVE_MIN_SECONDS = 300
BUDGET = 400

#Create a sql context
sc = SparkContext("spark://ip-172-31-0-101:7077", "nfl_plays")
sqlContext = SQLContext(sc)

#read in player salaries from hdfs
player_salaries_path = "hdfs://ec2-52-44-253-62.compute-1.amazonaws.com:9000/user/data/player_prices.dat"
salaries = sqlContext.read.json(player_salaries_path)
salaries.registerTempTable("salaries")

te_partition = '0'
qb_partition = '1'
wr_partition = '2'
rb_partition = '3'

def create_relevant_hdfs_filepath(est, position_partition):
    base = "hdfs://ec2-52-44-253-62.compute-1.amazonaws.com:9000/camus/topics/nfl_plays/hourly"
    year = est.strftime('%Y')
    month = est.strftime('%m')
    day = est.strftime('%d')
    hour = est.strftime('%H')
    return "{}/{}/{}/{}/{}/nfl_plays.{}?*".format(base, year, month, day, hour, position_partition)

def convert_datetime_to_est(current_time):
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/New_York')
    utc = current_time.replace(tzinfo=from_zone)
    return utc.astimezone(to_zone)

ct = datetime.datetime.now()
est = convert_datetime_to_est(ct)
epoch = int(time.mktime(est.timetuple()))

def generate_past_five_min_sql(epoch, position):
    query_text = "SELECT player_name, ( (SUM(yards) * .01) + (SUM(touchdown) * .01) ) as player_points from {}_plays \
                             WHERE epoch_timestamp > {} GROUP BY player_name ORDER BY player_points DESC".format(position, (epoch - FIVE_MIN_SECONDS))
    return query_text


#QUARTERBACKS
qb_filepath = create_relevant_hdfs_filepath(est, qb_partition)
qb_plays = sqlContext.read.json(qb_filepath)
qb_plays.registerTempTable("qb_plays")

top_qbs = sqlContext.sql(generate_past_five_min_sql(epoch, "qb"))
top_qbs.registerTempTable('top_qbs')
# join with salaries table
top_qbs_prices = sqlContext.sql("SELECT q.player_name, q.player_points, s.price FROM top_qbs q JOIN salaries s ON q.player_name = s.player_name")
top_qbs_prices.registerTempTable('top_qbs_prices')
top_ten_qbs = top_qbs_prices.take(10)


#TIGHTENDS
te_filepath = create_relevant_hdfs_filepath(est, te_partition)
te_plays = sqlContext.read.json(te_filepath)
te_plays.registerTempTable("te_plays")

top_tes = sqlContext.sql(generate_past_five_min_sql(epoch, "te"))
top_tes.registerTempTable('top_tes')
#join with salaries table
top_tes_prices = sqlContext.sql("SELECT t.player_name, t.player_points, s.price FROM top_tes t JOIN salaries s ON t.player_name = s.player_name")
top_tes_prices.registerTempTable('top_tes_prices')
top_ten_tes = top_tes_prices.take(10)


#WIDERECEIVERS
wr_filepath = create_relevant_hdfs_filepath(est, wr_partition)
wr_plays = sqlContext.read.json(wr_filepath)
wr_plays.registerTempTable("wr_plays")

top_wrs = sqlContext.sql(generate_past_five_min_sql(epoch, "wr"))
top_wrs.registerTempTable('top_wrs')
#join with salaries table
top_wrs_prices = sqlContext.sql("SELECT w.player_name, w.player_points, s.price FROM top_wrs w JOIN salaries s ON w.player_name = s.player_name")
top_wrs_prices.registerTempTable('top_wrs_prices')
top_ten_wrs = top_wrs_prices.take(10)


#RUNNING BACKS
rb_filepath = create_relevant_hdfs_filepath(est, rb_partition)
rb_plays = sqlContext.read.json(rb_filepath)
rb_plays.registerTempTable("rb_plays")

top_rbs = sqlContext.sql(generate_past_five_min_sql(epoch, "rb"))
top_rbs.registerTempTable('top_rbs')
#join with salaries table
top_rbs_prices = sqlContext.sql("SELECT r.player_name, r.player_points, s.price FROM top_rbs r JOIN salaries s ON r.player_name = s.player_name")
top_rbs_prices.registerTempTable('top_rbs_prices')
top_ten_rbs = top_rbs_prices.take(10)


# determine the highest scoring lineup under the budget
def calculate_player_salaries(qb_table, te_table, wr_table, rb_table, qb_i, te_i, wr_i, rb_i):
    return qb_table[qb_i]['price'] + te_table[te_i]['price'] + wr_table[wr_i]['price'] + rb_table[rb_i]['price']

def return_optimal_rdds(qb_table, te_table, wr_table, rb_table, qb_i, te_i, wr_i, rb_i):
    return qb_table[qb_i], te_table[te_i], wr_table[wr_i], rb_table[rb_i]

qb_i = 0
te_i = 0
wr_i = 0
rb_i = 0
over_budget = True
while over_budget:
    player_salaries = calculate_player_salaries(top_ten_qbs, top_ten_tes, top_ten_wrs, top_ten_rbs,
                                                qb_i, te_i, wr_i, rb_i)
    if player_salaries < BUDGET:
        over_budget = False
        optimal_qb_rdd, optimal_te_rdd, optimal_wr_rdd, optimal_rb_rdd = return_optimal_rdds(top_ten_qbs, top_ten_tes, top_ten_wrs, top_ten_rbs,
                                                qb_i, te_i, wr_i, rb_i)
        break
    qb_i += 1

    player_salaries = calculate_player_salaries(top_ten_qbs, top_ten_tes, top_ten_wrs, top_ten_rbs,
                                                qb_i, te_i, wr_i, rb_i)
    if player_salaries < BUDGET:
        over_budget = False
        optimal_qb_rdd, optimal_te_rdd, optimal_wr_rdd, optimal_rb_rdd = return_optimal_rdds(top_ten_qbs, top_ten_tes,
                                                                                             top_ten_wrs, top_ten_rbs,
                                                                                             qb_i, te_i, wr_i, rb_i)
        break
    te_i += 1

    player_salaries = calculate_player_salaries(top_ten_qbs, top_ten_tes, top_ten_wrs, top_ten_rbs,
                                                qb_i, te_i, wr_i, rb_i)
    if player_salaries < BUDGET:
        over_budget = False
        optimal_qb_rdd, optimal_te_rdd, optimal_wr_rdd, optimal_rb_rdd = return_optimal_rdds(top_ten_qbs, top_ten_tes,
                                                                                             top_ten_wrs, top_ten_rbs,
                                                                                             qb_i, te_i, wr_i, rb_i)
        break
    wr_i += 1

    player_salaries = calculate_player_salaries(top_ten_qbs, top_ten_tes, top_ten_wrs, top_ten_rbs,
                                                qb_i, te_i, wr_i, rb_i)
    if player_salaries < BUDGET:
        over_budget = False
        optimal_qb_rdd, optimal_te_rdd, optimal_wr_rdd, optimal_rb_rdd = return_optimal_rdds(top_ten_qbs, top_ten_tes,
                                                                                             top_ten_wrs, top_ten_rbs,
                                                                                             qb_i, te_i, wr_i, rb_i)
        break
    rb_i += 1

optimal_df = sqlContext.createDataFrame([optimal_qb_rdd, optimal_te_rdd, optimal_wr_rdd, optimal_rb_rdd])
optimal_df.registerTempTable('optimal_df')

optimal_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode('overwrite') \
    .options(table="optimal_lineup", keyspace="nfl_plays") \
    .save()


