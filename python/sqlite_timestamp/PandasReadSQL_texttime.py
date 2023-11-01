import argparse
import logging
import os
from typing import Optional, Tuple

import sqlite3
from sqlite3 import Error

import pandas as pd
from pandas.core.frame import DataFrame

"""
UDPパケットを登録したSQLite3データベースからPandas DataFrameを取得する
測定時刻(measurement_time): TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS").
"""

# スクリプト名
script_name = os.path.basename(__file__)
# ログフォーマット
LOG_FMT = '%(levelname)s %(message)s'

# インデックス
COL_TIME: str = "measurement_time"

# 気象データINSERT ※measurement_timeは"unixepoch"+"localtime"で登録される
# https://docs.python.org/ja/3/library/sqlite3.html
#  sqlite3 --- SQLite データベースに対する DB-API 2.0 インターフェース
#    接続(connection)コンテキストマネージャの使い方
"""
(登録SQL: INSERT_WEATHER)
  INSERT INTO t_weather(did, measurement_time, temp_out, temp_in, humid, pressure) 
  VALUES (?, ?, ?, ?, ?, ?)  
(レコード登録)
    conn = get_connection(weather_db, logger=logger)
    did = get_did(conn, device_name, logger=logger)
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
    except sqlite3.Error as err:
        if logger is not None:
            logger.warning("rec: {}\nerror:{}".format(rec, err))
    finally:
        if conn is not None:
           conn.close()
"""

QUERY_RANGE_DATA: str = """
SELECT
   datetime(measurement_time) as measurement_time
   ,temp_out, humid, pressure
FROM
   t_weather
WHERE
   did=(SELECT id FROM t_device WHERE name=?)
   AND (
      measurement_time >= ? AND measurement_time < ?
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


def get_dataframe(connection: sqlite3.Connection,
                  device_name: str, from_datetime: str, to_datetime: str,
                  logger: Optional[logging.Logger] = None) -> DataFrame:
    query_params: Tuple = (
        device_name, from_datetime, to_datetime,
    )
    if logger is not None:
        logger.info(f"query_params: {query_params}")
    df: pd.DataFrame = pd.read_sql(
        QUERY_RANGE_DATA, connection, params=query_params, parse_dates=[COL_TIME]
    )
    return df


if __name__ == '__main__':
    logging.basicConfig(format=LOG_FMT)
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.DEBUG)

    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    # 測定時刻がTEXT型SQLite3 データベースパス
    #  for text time:~/db/weather_textime_db
    parser.add_argument("--sqlite3-db", type=str, required=True,
                        help="測定時刻がTEXT型の SQLite3 データベースパス")
    # デバイス名: esp8266_1
    parser.add_argument("--device-name", type=str, required=True,
                        help="device name in t_device.")
    # 検索開始時刻(含む)
    parser.add_argument("--from-datetime", type=str, required=True,
                        help="(example) 2023-10-30 10:00:00")
    # 検索終了時刻(含まない)
    parser.add_argument("--to-datetime", type=str, required=True,
                        help="(example) 2023-10-30 12:00:00")
    args: argparse.Namespace = parser.parse_args()
    # データベースパス
    db_path: str = os.path.expanduser(args.sqlite3_db)
    if not os.path.exists(db_path):
        app_logger.warning("database not found!")
        exit(1)

    # デバイス名
    param_device_name: str = args.device_name
    # 検索開始時刻(含む)
    param_from_datetime: str = args.from_datetime
    # 検索終了時刻(含まない)
    param_to_datetime: str = args.to_datetime

    conn: sqlite3.Connection = Optional[None]
    try:
        conn = get_connection(db_path, read_only=True)
        app_logger.info(f"connection: {conn}")
        read_df: DataFrame = get_dataframe(
            conn, param_device_name,
            param_from_datetime, param_to_datetime, logger=app_logger)
        rec_count: int = read_df.shape[0]
        app_logger.info(f"rec_count: {rec_count}")
        if rec_count > 0:
            app_logger.info(f"{read_df}")
    except Exception as err:
        app_logger.warning(err)
        exit(1)
    finally:
        if conn is not None:
            conn.close()
