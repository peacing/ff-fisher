                                try:
                                    touchdown = statistic['touchdown']
                                except:
                                    touchdown = 0
                                plays.append([player_name] + [player_id] + [yards] + [touchdown])
                        except:
                            continue
                except:
                    continue


df = pd.DataFrame(plays, columns=['player_name', 'player_id','yards','touchdown'])

class Producer:

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_messages(self, data):
        timestamp = 1473613200 # 1:00 est
        while timestamp <= 1473624000:
            rows = np.random.randint(0,len(data)-1, size=num_plays_persec)
            sampled_data = data.iloc[rows]

            for idx, row in sampled_data.iterrows():
                str_fmt = "{};{};{};{}:{}"
                message_info = str_fmt.format(timestamp,
                                              row.player_id,
                                              row.player_name,
                                              row.yards,
                                              row.touchdown)
                key = random.randint(1,8)
                self.producer.send_messages('nfl_week1_data', str(key), message_info)
            timestamp += 1


num_plays_persec = 100
ip_addr = '52.44.253.62'

prod = Producer(ip_addr)
prod.produce_messages(df)