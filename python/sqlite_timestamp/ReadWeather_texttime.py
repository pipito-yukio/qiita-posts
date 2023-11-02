import argparse
import logging
import os
from typing import Optional, Tuple

import sqlite3
from sqlite3 import Error

"""
UDPパケットを登録したSQLite3データベースの気象データを出力する
測定時刻(measurement_time): TEXT (iso8601)
"""
# ログフォーマット
LOG_FMT: str = '%(levelname)s %(message)s'

# (登録) measurement_time: TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS")
"""
(登録SQL: INSERT_WEATHER)
  INSERT INTO t_weather(did, measurement_time, temp_out, temp_in, humid, pressure) 
  VALUES (?, ?, ?, ?, ?, ?)  
(レコード登録)
    rec = (did,
           measurement_time,
           to_float(temp_out),
           to_float(temp_in),
           to_float(humid),
           to_float(pressure)
           )
    try:
        with conn:
            conn.execute(INSERT_WEATHER, rec)
"""

# 気象センサーデバイス名と期間から気象観測データを取得するSQL (SQLite3専用)
QUERY_RANGE_DATA: str = """
SELECT
   measurement_time, temp_out, temp_in, humid, pressure
FROM
   t_weather
WHERE
   did=(SELECT id FROM t_device WHERE name=?)
   AND (
      measurement_time >= ? AND  measurement_time <= ?
   )
ORDER BY measurement_time;
"""


def get_connection(db_file_path: str,
                   auto_commit=False, read_only=False, logger=None) -> sqlite3.Connection:
    try:
        if read_only:
            db_uri = "file://{}?mode=ro".format(db_file_path)
            connection = sqlite3.connect(db_uri, uri=True)
        else:
            connection = sqlite3.connect(db_file_path)
            if auto_commit:
                connection.isolation_level = None
    except Error as e:
        if logger is not None:
            logger.error(e)
        raise e
    return connection


if __name__ == '__main__':
    logging.basicConfig(format=LOG_FMT)
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.DEBUG)

    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    # 測定時刻がTEXT型SQLite3 データベースパス
    #  for iso8601 text time:~/db/weather_texttime_db
    parser.add_argument("--sqlite3-db", type=str, required=True,
                        help="測定時刻が TEXT型の SQLite3 データベースパス")
    # デバイス名: esp8266_1
    parser.add_argument("--device-name", type=str, required=True,
                        help="device name in t_device.")
    # 検索開始時刻
    parser.add_argument("--from-datetime", type=str, required=True,
                        help="(example) 2023-10-30 10:00:00")
    # 検索終了時刻
    parser.add_argument("--to-datetime", type=str, required=True,
                        help="(example) 2023-10-30 12:00:00")
    args: argparse.Namespace = parser.parse_args()
    # データベースパス
    db_path: str = os.path.expanduser(args.sqlite3_db)
    if not os.path.exists(db_path):
        app_logger.warning("database not found!")
        exit(1)

    conn: sqlite3.Connection = Optional[None]
    try:
        conn = get_connection(db_path, logger=app_logger)
        cursor: sqlite3.Cursor = conn.cursor()
        try:
            sql_param: Tuple = (
                args.device_name, args.from_datetime, args.to_datetime
            )
            app_logger.info(sql_param)
            cursor.execute(QUERY_RANGE_DATA, sql_param)
            # 測定時刻(文字列),外気温,室内気温,室内湿度,気圧
            for rec in cursor:
                app_logger.info(f'"{rec[0]}",{rec[1]},{rec[2]},{rec[3]},{rec[4]}')
        finally:
            cursor.close()
    except Exception as err:
        app_logger.warning(err)
        exit(1)
    finally:
        if conn is not None:
            conn.close()
