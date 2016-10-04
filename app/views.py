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
    SQL = "SELECT * FROM micro_winner"
    response = session.execute(SQL)
    user_id = response[0].user_id
    user_points = response[0].user_points
    return render_template("base.html", user_id=user_id, user_points=user_points)


@app.route('/<user_id>')
def get_email(user_id):
       stmt = "SELECT * FROM micro_winners WHERE user_id=%s"
       response = session.execute(stmt, parameters=[user_id])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"user_id": x.user_id, "user_points": x.user_points} for x in response_list]
       return jsonify(emails=jsonresponse)


@app.route('/_get_data')
def get_data():
    SQL = "SELECT * FROM micro_winner"
    response = session.execute(SQL)
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"user_id": x.user_id, "user_points": x.user_points} for x in response_list]
    return jsonify(winner=jsonresponse)