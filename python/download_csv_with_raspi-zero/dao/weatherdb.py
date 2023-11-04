import logging
import sqlite3
from datetime import date
from typing import Iterator, List, Optional, Tuple

"""
Weather database CRUD functions, Finder class 
"""


def get_connection(db_path: str,
                   auto_commit: bool = False, read_only: bool = False,
                   logger: Optional[logging.Logger] = None) -> sqlite3.Connection:
    try:
        conn: sqlite3.Connection
        if read_only:
            db_uri: str = "file://{}?mode=ro".format(db_path)
            conn = sqlite3.connect(db_uri, uri=True)
        else:
            conn = sqlite3.connect(db_path)
            if auto_commit:
                conn.isolation_level = None
    except sqlite3.Error as e:
        if logger is not None:
            logger.error(e)
        raise e
    return conn


def find_device(conn: sqlite3.Connection, device_name: str,
                logger: Optional[logging.Logger] = None, log_level_debug: bool = False
                ) -> Optional[int]:
    rec: Optional[Tuple[int]]
    with conn:
        cur: sqlite3.Cursor = conn.execute(
            "SELECT id FROM t_device WHERE name = ?", (device_name,)
        )
        rec = cur.fetchone()
    if logger is not None and log_level_debug:
        logger.debug("{}: {}".format(device_name, rec))
    # これ以上データがない場合は Noneを返す
    if rec is not None:
        return rec[0]

    return None


class WeatherFinder:
    # Private constants
    _SELECT_WEATHER_COUNT: str = """
SELECT
   COUNT(*)
FROM
   t_weather
WHERE
   did = ?
   AND (
      measurement_time >= strftime('%s', ? ,'-9 hours')
      AND
      measurement_time < strftime('%s', ? ,'-9 hours')
   )
"""
    _SELECT_WEATHER: str = """
SELECT
   did, datetime(measurement_time, 'unixepoch', 'localtime'), temp_out, temp_in, humid, pressure
FROM
   t_weather
WHERE
   did = ?
   AND (
      measurement_time >= strftime('%s', ? ,'-9 hours')
      AND
      measurement_time < strftime('%s', ? ,'-9 hours')
   )
ORDER BY measurement_time;
    """
    # if record count > GENERATOR_THRETHOLD then CSV Generator else CSV list
    _GENERATOR_WEATHER_THRESHOLD: int = 10000
    _GENERATOR_WEATHER_BATCH_SIZE: int = 1000
    # CSV constants
    _FMT_WEATHER_CSV_LINE = '{},"{}",{},{},{},{}'
    # Public const
    # CSV t_weather Header
    CSV_WEATHER_HEADER = '"did","measurement_time","temp_out","temp_in","humid","pressure"\n'

    def __init__(self, db_path: str, logger: Optional[logging.Logger] = None):
        self.logger = logger
        if logger is not None and (logger.getEffectiveLevel() <= logging.DEBUG):
            self.isLogLevelDebug = True
        else:
            self.isLogLevelDebug = False
        self.db_path: str = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        self.csv_iter = None
        self._csv_name: Optional[str] = None

    def close(self):
        """ Close cursor and connection close """
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()

    @property
    def csv_filename(self) -> str:
        """ CSV filename: 'weather_[device_name]_[from_date]-[to_date]_[today].csv' """
        return "weather_{}.csv".format(self._csv_name)

    def _csv_iterator(self) -> Iterator[str]:
        """
        Generate Csv generator
          line: did, "YYYY-mm-DD HH:MM:SS(measurement_time)",temp_out,temp_in,humid,pressure
          (*) temp_out,temp_in,humid, pressure: if filedValue is None then empty string
        :return: Record generator
        """
        while True:
            batch_resords: Optional[List[tuple]] = self.cursor.fetchmany(
                self._GENERATOR_WEATHER_BATCH_SIZE)
            if not batch_resords:
                break
            for rec in batch_resords:
                yield self._FMT_WEATHER_CSV_LINE.format(rec[0],
                                                        rec[1],
                                                        rec[2] if rec[2] is not None else '',
                                                        rec[3] if rec[3] is not None else '',
                                                        rec[4] if rec[4] is not None else '',
                                                        rec[5] if rec[5] is not None else '')

    def _csv_list(self) -> List[str]:
        """
        Get CSV list
          line: did, "YYYY-mm-DD HH:MM:SS(measurement_time)",temp_out,temp_in,humid,pressure
          (*) temp_out,temp_in,humid, pressure: if filedValue is None then empty string
        :return: Record list, if no record then blank list
        """
        return [self._FMT_WEATHER_CSV_LINE.format(rec[0],
                                                  rec[1],
                                                  rec[2] if rec[2] is not None else '',
                                                  rec[3] if rec[3] is not None else '',
                                                  rec[4] if rec[4] is not None else '',
                                                  rec[5] if rec[5] is not None else '') for rec in self.cursor]

    def find(self, device_name: str, date_from: str, date_to: str) -> Iterator[str] | List[str]:
        # Build csv filename suffix
        date_part: date = date.today()
        name_suffix: str = "{}_{}_{}".format(
            device_name, date_from.replace("-", ""), date_to.replace("-", "")
        )
        self._csv_name = name_suffix + "_" + date_part.strftime("%Y%m%d")

        if self.conn is None:
            self.conn = get_connection(self.db_path, read_only=True, logger=self.logger)
        did: Optional[int] = find_device(self.conn, device_name, logger=self.logger)
        if did is None:
            return []

        params: Tuple = (did, date_from, date_to)
        try:
            # Check record count
            self.cursor = self.conn.cursor()
            self.cursor.execute(self._SELECT_WEATHER_COUNT, params)
            # fetchone() return tuple (?,)
            row_count: int = self.cursor.fetchone()[0]
            if self.logger is not None:
                self.logger.info("Record count: {}".format(row_count))
            if row_count == 0:
                return []

            # Get record
            self.cursor.execute(self._SELECT_WEATHER, params)
            if row_count > self._GENERATOR_WEATHER_THRESHOLD:
                if self.logger is not None:
                    self.logger.info("Return CSV iterator")
                # make generator
                return self._csv_iterator()
            else:
                if self.logger is not None:
                    self.logger.info("Return CSV list")
                return self._csv_list()
        except sqlite3.Error as err:
            if self.logger is not None:
                self.logger.warning("criteria: {}\nerror:{}".format(params, err))
            raise err
