# Fantasy Football Fisher - Get hooked on a new fantasy football offering.

FF Fisher is an app that processes in real-time a continuous stream of football play data. Displayed on the app are top-scoring users their point totals.

FF Fisher is powered by the following distributed open-source technologies:

* Kafka
* Spark
* Cassandra
* Flask

### Pipeline

The architecture looks as shown in the diagram below:

![Pipeline](http://i.imgur.com/BfdWMSW.png)

### Data Generation and Ingestion

Play-by-Play files from the SportRadar API provide raw input. Processed data contains JSON records of the player name, player position, the number of yards, whether the play contained a touchdown, and a timestamp. Since NFL plays do not happen continuously, a manufactured stream of plays from the list of actual NFL plays is created at the rate of 100 per second.

Each play is placed onto a Kafka topic and is sent to a Spark Streaming Context.

### Micro Leagues (Spark Windowed Streaming)

With FF-Fisher's Micro-leagues, there's a new winner every 30 seconds! Behind the scenes, users' point totals are aggregated from the last 30 seconds of plays via a windowed spark stream. At the conclusion of the 30 second window, the top scoring user is identified and his or her user_id and point total are saved to the database.

### Player Point Updates

Every second, players point totals are incrementally updated as the latest plays occur. This process leverages prepared statements to optimize query execution and limit the load on both the Spark Streaming and Cassandra clusters. Data is stored in Cassandra with Player Name as the key, the Timestamp of the play as the sorted key, and the Player Points as the value. This makes querying for the latest plays of a given player lightning fast!

### Front end
All winners of micro leagues are displayed on our homepage, www.fantasyfootballfisher.win.
As well as up-to-the-latest second player point totals, searchable from the Players tab.


