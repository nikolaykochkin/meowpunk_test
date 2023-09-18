import csv
import logging
import argparse
import sqlite3
from typing import List, Dict
from calendar import timegm
from datetime import datetime, date, time
from time import time as now

SELECT_CHEATERS = "SELECT player_id, ban_time FROM cheaters"
DROP_RESULT_TABLE = "DROP TABLE IF EXISTS RESULT"
CREATE_RESULT_TABLE = \
    "CREATE TABLE IF NOT EXISTS RESULT (TIMESTAMP, player_id, event_id, error_id, json_server, json_client)"
INSERT_RESULT = "INSERT INTO result VALUES (?, ?, ?, ?, ?, ?)"

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


class Client:
    def __init__(self, timestamp: int, error_id: str, player_id: int, description: str):
        self.timestamp = timestamp
        self.error_id = error_id
        self.player_id = player_id
        self.description = description


class Server:
    def __init__(self, timestamp: int, event_id: int, error_id: str, description: str):
        self.timestamp = timestamp
        self.event_id = event_id
        self.error_id = error_id
        self.description = description


def read_client_file() -> List[Client]:
    result = []
    with open(config.client_path) as clients_csv:
        reader = csv.reader(clients_csv)
        first = True
        for row in reader:
            if first:
                first = False
                continue
            ts = int(row[0])
            if config.start_ts <= ts <= config.end_ts:
                result.append(Client(ts, row[1], int(row[2]), row[3]))
    return result


def read_server_file() -> Dict[str, Server]:
    result = {}
    with open(config.server_path) as server_csv:
        reader = csv.reader(server_csv)
        first = True
        for row in reader:
            if first:
                first = False
                continue
            ts = int(row[0])
            if config.start_ts <= ts <= config.end_ts:
                result[row[2]] = Server(ts, int(row[1]), row[2], row[3])
    return result


def read_cheaters() -> Dict[int, str]:
    result = {}
    with sqlite3.connect(config.cheaters_path) as conn:
        cursor = conn.execute(SELECT_CHEATERS)
        for row in cursor:
            result[int(row[0])] = row[1]
    return result


def write_result():
    prev_day_ts = config.start_ts - 86400
    batch = []
    batch_num = 1
    row_count = 0
    with sqlite3.connect(config.result_path) as conn:
        conn.execute(DROP_RESULT_TABLE)
        conn.execute(CREATE_RESULT_TABLE)
        for client_row in client:
            server_row = server.get(client_row.error_id)
            ban_time = cheaters.get(client_row.player_id)
            if server_row:
                if ban_time:
                    ban_ts = timegm(datetime.fromisoformat(ban_time).timetuple())
                    if prev_day_ts <= ban_ts < config.start_ts or ban_ts < server_row.timestamp:
                        continue
                batch.append((server_row.timestamp, client_row.player_id, server_row.event_id, client_row.error_id,
                              server_row.description, client_row.description))
                row_count += 1
                if len(batch) >= config.batch_size:
                    logging.info("Writing batch #%s", batch_num)
                    conn.executemany(INSERT_RESULT, batch)
                    batch.clear()
                    batch_num += 1
        if len(batch) > 0:
            logging.info("Writing batch #%s", batch_num)
            conn.executemany(INSERT_RESULT, batch)
    logging.info("Total result rows %s", row_count)


if __name__ == '__main__':
    start = now()
    config = Configuration.from_console_args()
    logging.info("Start loading with config %s", config)

    logging.info("Read client file")
    client = read_client_file()
    logging.info("Total client rows %s", len(client))

    logging.info("Read server file")
    server = read_server_file()
    logging.info("Total server rows %s", len(server))

    logging.info("Read cheaters db")
    cheaters = read_cheaters()
    logging.info("Total cheaters rows %s", len(cheaters))

    logging.info("Start writing results")
    write_result()
    logging.info("Loading completed")
    end = now()
    logging.info("Time taken = %s sec", str(end - start))

