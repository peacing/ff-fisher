import json

file = 'week_1/week1_games.json'

with open(file, 'r') as input:
    week1_json = json.load(input)
    games = week1_json['weeks'][0]['games']
    week1_game_ids = []
    for game in games:
        id = game['id']
        week1_game_ids.append(id)
