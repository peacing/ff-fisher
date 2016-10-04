from pyspark.sql import SQLContext
from pyspark import SparkContext

#Create a sql context
sc = SparkContext("spark://ip-172-31-0-106:7077", "nfl_plays")
sqlContext = SQLContext(sc)

users_path = "hdfs://ec2-52-45-144-162.compute-1.amazonaws.com:9000/user/users_players100.dat"
users = sqlContext.read.json(users_path)

users.registerTempTable("users")

users.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode('overwrite') \
    .options(table="micro_users", keyspace="nfl_plays") \
    .save()