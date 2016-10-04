import json
import pandas as pd
import numpy as np
import glob
import datetime

files = glob.glob('week_1/*.json')
dfs = []
for file in files:
    plays = []
    with open(file, 'r') as input:
        data = json.load(input)
        periods = data['periods']
        for period in periods:
            pbp = period['pbp']
            for i in range(1, len(pbp)):
                try:
                    events = pbp[i]['events']
                    for play in events:
                        try:
                            statistics = play['statistics']
                            for statistic in statistics:
                                try:
                                    stat_type = statistic['stat_type']
                                    if stat_type == 'rush':
                                        yards = statistic['yards']
                                        player_id = statistic['player']['id']
                                        player_name = statistic['player']['name']
                                        position = statistic['player']['position']
                                        try:
                                            touchdown = statistic['touchdown']
                                        except:
                                            touchdown = 0
                                        plays.append([player_name] + [player_id] + [position] + [yards] + [touchdown])
                                    if stat_type == 'pass':
                                        if statistic['complete'] == 0:
                                            continue
                                        else:
                                            yards = statistic['yards']
                                            player_id = statistic['player']['id']
                                            player_name = statistic['player']['name']
                                            position = statistic['player']['position']
                                        try:
                                            touchdown = statistic['touchdown']
                                        except:
                                            touchdown = 0
                                        plays.append([player_name] + [player_id] + [position] + [yards] + [touchdown])
                                    if stat_type == 'receive':
                                        # if statistic['reception']:
                                        yards = statistic['yards']
                                        player_id = statistic['player']['id']
                                        player_name = statistic['player']['name']
                                        position = statistic['player']['position']
                                        try:
                                            touchdown = statistic['touchdown']
                                        except:
                                            touchdown = 0
                                            plays.append([player_name] + [player_id] + [position] + [yards] + [touchdown])
                                except:
                                    continue
                        except:
                            continue
                except:
                    continue

    df = pd.DataFrame(plays, columns=['player_name', 'player_id', 'position', 'yards', 'touchdown'])
    dfs.append(df)

all_plays = pd.concat(dfs)

all_plays.to_csv('all_plays.csv', index=False)

print(datetime.datetime.now())