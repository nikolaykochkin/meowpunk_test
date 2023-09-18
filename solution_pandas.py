import pandas as pd
import logging
import argparse
import sqlite3
from datetime import datetime, date, time
from calendar import timegm
from time import time as now

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description="Client server data parser")
parser.add_argument("-c", "--client", default="data/client.csv", help="Client csv file path")
parser.add_argument("-s", "--server", default="data/server.csv", help="Server csv file path")
parser.add_argument("-t", "--cheater", default="data/cheaters.db", help="Cheaters db file path")
parser.add_argument("-r", "--result", default="data/result.db", help="Result db file path")
parser.add_argument("-d", "--day", default="2021-05-18", help="Load date in format '2021-05-18'")


class Configuration:
    def __init__(self, client_path: str, server_path: str, cheaters_path: str, result_path: str, day: str,
                 batch_size=100):
        self.client_path = client_path
        self.server_path = server_path
        self.cheaters_path = cheaters_path
        self.result_path = result_path
        self.day = date.fromisoformat(day)
        self.start_ts = timegm(self.day.timetuple())
        self.end_ts = timegm(datetime.combine(self.day, time.max).timetuple())
        self.batch_size = batch_size

    @classmethod
    def from_console_args(cls):
        args = parser.parse_args()
        return cls(args.client, args.server, args.cheater, args.result, args.day)

    def __repr__(self):
        return str(vars(self))


if __name__ == '__main__':
    start = now()
    config = Configuration.from_console_args()
    logging.info("Start loading with config %s", config)

    logging.info("Read client file")
    client = pd.read_csv(config.client_path).query(f'{config.start_ts} <= timestamp <= {config.end_ts}')
    logging.info("Total client rows %s", len(client))

    logging.info("Read server file")
    server = pd.read_csv(config.server_path).query(f'{config.start_ts} <= timestamp <= {config.end_ts}')
    logging.info("Total server rows %s", len(server))

    logging.info("Read cheaters db")
    cnx = sqlite3.connect(config.cheaters_path)
    cheaters = pd.read_sql("SELECT player_id, ban_time FROM cheaters", cnx)
    cheaters['ban_ts'] = (pd.to_datetime(cheaters.ban_time) - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")
    cnx.close()
    logging.info("Total cheaters rows %s", len(cheaters))

    prev_day_ts = config.start_ts - 86400
    result = pd.merge(client, server, on='error_id', suffixes=("_client", "_server"))
    result = pd.merge(result, cheaters, how='left', on='player_id') \
        .query(f"ban_ts.isnull() or ban_ts > timestamp_server") \
        .drop(['timestamp_client', 'ban_time', 'ban_ts'], axis=1) \
        .rename(columns={"timestamp_server": "timestamp", "description_client": "json_client",
                         "description_server": "json_server"})

    conn = sqlite3.connect(config.result_path)
    conn.execute("DROP TABLE IF EXISTS result")
    result.to_sql("result", conn, index=False, if_exists='replace', chunksize=config.batch_size)
    conn.close()

    logging.info("Result rows %s", len(result))

    logging.info("Loading completed")
    end = now()
    logging.info("Time taken = %s sec", str(end - start))
