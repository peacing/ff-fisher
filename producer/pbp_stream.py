from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

import json
import pandas as pd
import numpy as np
import random
import six
import time
import datetime
from dateutil import tz


df = pd.read_csv('all_plays.csv')

def convert_datetime_to_est(current_time):
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/New_York')
    utc = current_time.replace(tzinfo=from_zone)
    return utc.astimezone(to_zone)

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_messages(self, data):
        #timestamp = 1473613200 # 1:00 est
        #while timestamp <= 1473624000:
        while True:
            rows = np.random.randint(0,len(data)-1, size=num_plays_persec)
            sampled_data = data.iloc[rows]
            curr_time = datetime.datetime.now()
            #create timestamp for camus to partition
            timestamp = datetime.datetime.strftime(curr_time, '%Y-%m-%d_%H:%M:%S')
            #create epoch timstamp for custom partitioning
            raw_timestamp = convert_datetime_to_est(curr_time)
            epoch = int(time.mktime(raw_timestamp.timetuple()))
            for idx, row in sampled_data.iterrows():
                json_data = {'timestamp': timestamp, 'epoch_timestamp': epoch,'player_id': row.player_id, 'player_name': row.player_name,
                        'position': row.position, 'yards': row.yards, 'touchdown': row.touchdown}
                message_info = json.dumps(json_data)
                keystring = 'QA' if row.position == 'QB' else row.position
                key =  b'{}'.format(keystring)
                self.producer.send_messages('nfl_plays', key, message_info)


num_plays_persec = 1000
ip_addr = '52.44.253.62'

prod = Producer(ip_addr)
prod.produce_messages(df)