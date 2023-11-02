import argparse
import os
import logging
import socket
import sqlite3
from datetime import datetime
from typing import List, Optional, Tuple

"""
UDP packet monitor from ESP Weather sensors With Insert weather_db on SQlite3 database
[UDP port] 2222
measurement_time: TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS").
"""

# ログフォーマット
LOG_FMT: str = '%(levelname)s %(message)s'
# SQL定義
FIND_DEVICE: str = "SELECT id FROM t_device WHERE name = ?"
INSERT_WEATHER: str = """
INSERT INTO t_weather(did, measurement_time, temp_out, temp_in, humid, pressure) 
 VALUES (?, ?, ?, ?, ?, ?)  
"""

# args option default
WEATHER_UDP_PORT: int = 2222
BUFF_SIZE: int = 1024
# UDP packet receive timeout 12 minutes
RECV_TIMEOUT: float = 12. * 60


# https://www.sqlite.org/datatype3.html
# Datatypes In SQLite
#  2.2. Date and Time Datatype
#   (1) TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS").
#   (2) REAL as Julian day numbers, the number of days since noon in Greenwich
#      on November 24, 4714 B.C. according to the proleptic Gregorian calendar.
#   (3) INTEGER as Unix Time, the number of seconds since 1970-01-01 00:00:00 UTC.


def to_float(s_value: str) -> Optional[float]:
    """
    Numeric string convert to float value
    :param s_value: Numeric string
    :return: float value or if ValueError, None
    """
    try:
        val = float(s_value)
    except ValueError:
        val = None
    return val


def get_connection(db_file_path: str, auto_commit: bool = False, read_only: bool = False,
                   logger: Optional[logging.Logger] = None) -> sqlite3.Connection:
    try:
        if read_only:
            db_uri = "file://{}?mode=ro".format(db_file_path)
            connection = sqlite3.connect(db_uri, uri=True)
        else:
            connection = sqlite3.connect(db_file_path)
            if auto_commit:
                connection.isolation_level = None
    except sqlite3.Error as e:
        if logger is not None:
            logger.error(e)
        raise e
    return connection


def find_device(conn: sqlite3.Connection, device_name: str,
                logger: Optional[logging.Logger] = None, log_level_debug: bool = False
                ) -> Optional[int]:
    """
    Check device name in t_device.
    :param conn: Weather database connection
    :param device_name: Device name
    :param logger: application logger or None
    :param log_level_debug: logger is not None and logLevel=DEBUG
    :return: if exists then Device ID else None
    """
    rec: Optional[Tuple[int]]
    with conn:
        cur: sqlite3.Cursor = conn.execute(FIND_DEVICE, (device_name,))
        rec = cur.fetchone()
    if logger is not None and log_level_debug:
        logger.debug("{}: {}".format(device_name, rec))
    # https://docs.python.org/ja/3/library/sqlite3.html
    # これ以上データがない場合は Noneを返す
    if rec is not None:
        return rec[0]

    return None


def insert(device_name: str, temp_out: str, temp_in: str, humid: str, pressure: str,
           measurement_time: str, logger: Optional[logging.Logger] = None) -> None:
    """
    Insert weather sensor data to t_weather
    :param device_name: device name (required)
    :param temp_out: Outdoor Temperature (float or None)
    :param temp_in: Indoor Temperature (float or None)
    :param humid: humidity (float or None)
    :param pressure: pressure (float or None)
    :param measurement_time: ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS").
    :param logger: application logger or None
    """
    log_level_debug: bool = False
    if logger is not None:
        log_level_debug = logger.getEffectiveLevel() <= logging.DEBUG
    conn: sqlite3.Connection = get_connection(weather_db, logger=logger)
    did: Optional[int] = find_device(conn, device_name, logger=logger)
    if did is None:
        warning: str = f"{device_name} not found!"
        if logger is not None:
            logger.warning(warning)
        else:
            print(warning)
        return

    rec: Tuple[int, str, Optional[float], Optional[float], Optional[float], Optional[float]] = (
        did,
        measurement_time,
        to_float(temp_out),
        to_float(temp_in),
        to_float(humid),
        to_float(pressure)
    )
    if logger is not None and log_level_debug:
        logger.debug(rec)
    try:
        with conn:
            conn.execute(INSERT_WEATHER, rec)
    except sqlite3.Error as db_err:
        error: str = f"rec: {rec}\nerror:{db_err}"
        if logger is not None:
            logger.warning(error)
        else:
            print(error)
    finally:
        if conn is not None:
            conn.close()


def loop(client: socket.socket):
    server_ip: str = ''
    # Timeout setting
    client.settimeout(RECV_TIMEOUT)
    data: bytes
    address: str
    while True:
        try:
            data, addr = client.recvfrom(BUFF_SIZE)
            if server_ip != addr:
                server_ip = addr
                app_logger.info("server ip: {}".format(server_ip))

            # from ESP output: device_name, temp_out, temp_in, humid, pressure
            line: str = data.decode("utf-8")
            record: List[str] = line.split(",")
            # Insert weather DB
            # measurement_time: ISO8601 ("YYYY-MM-DD HH:MM:SS.SSS")
            now: datetime = datetime.now()
            s_timestamp: str = now.isoformat(sep=' ', timespec='milliseconds')
            app_logger.debug(f"{s_timestamp}")
            insert(record[0], record[1], record[2], record[3], record[4],
                   measurement_time=s_timestamp, logger=app_logger)
        except socket.timeout as timeout:
            app_logger.warning(timeout)
            raise timeout


if __name__ == '__main__':
    logging.basicConfig(format=LOG_FMT)
    app_logger: logging.Logger = logging.getLogger(__name__)
    app_logger.setLevel(level=logging.DEBUG)

    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    # 測定時刻がTEXT型SQLite3 データベースパス
    #  for text time:~/db/weather_textime_db
    parser.add_argument("--sqlite3-db", type=str, required=True,
                        help="測定時刻がTEXT型の SQLite3 データベースパス")
    args: argparse.Namespace = parser.parse_args()
    # データベースパス
    weather_db: str = os.path.expanduser(args.sqlite3_db)
    if not os.path.exists(weather_db):
        app_logger.warning("database not found!")
        exit(1)

    # Receive broadcast.
    broad_address = ("", WEATHER_UDP_PORT)
    app_logger.info(f"Listen: {broad_address}")
    # UDP client
    udp_client: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    app_logger.info(f"udp_client: {udp_client}")
    udp_client.bind(broad_address)
    try:
        loop(udp_client)
    except KeyboardInterrupt:
        pass
    except Exception as err:
        app_logger.error(err)
    finally:
        udp_client.close()
