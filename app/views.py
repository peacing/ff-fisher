from flask import jsonify, request, render_template
from app import app
from flask import render_template
# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra
cluster = Cluster(["ec2-52-44-4-126.compute-1.amazonaws.com"])
session = cluster.connect("nfl_plays")

@app.route('/')
@app.route('/index')
def index():
    return render_template("welcome.html")

@app.route('/winner')
def winner():
    SQL = "SELECT * FROM micro_winner2"
    response = session.execute(SQL)
    user_id = response[0].user_id
    user_points = response[0].user_points
    return render_template("winner.html", user_id=user_id, user_points=user_points)

@app.route('/player')
def winners():
    return render_template("player.html")

@app.route('/player', methods=['POST'])
def player_post():
    player_id = request.form["player_id"]
    SQL = "SELECT * FROM player_scores WHERE player_name=%s LIMIT 25"
    response = session.execute(SQL, parameters=[player_id])
    response_list = []
    for val in response:
        response_list.append(val)
    json_response = [{"player_name": x.player_name, "position": x.position, "player_points": x.player_points, "yards": x.yards,"touchdown": x.touchdown, "time": x.playtime} for x in response_list]
    return render_template('playerop.html', output=json_response)