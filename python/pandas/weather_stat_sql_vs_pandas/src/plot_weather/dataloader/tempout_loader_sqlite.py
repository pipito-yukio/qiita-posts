import sqlite3
from typing import Tuple

import pandas as pd
from pandas.core.frame import DataFrame

"""
SQLite3 気象データの外気温ロードモジュール
"""


COL_TIME: str = "measurement_time"
COL_TEMP_OUT: str = "temp_out"


_QUERY: str = """
SELECT
   datetime(measurement_time,'unixepoch', 'localtime') as measurement_time,
   temp_out
FROM
   t_weather tw INNER JOIN t_device td ON tw.did=td.id
WHERE
   td.name = ?
   AND (
       datetime(measurement_time,'unixepoch', 'localtime') >= ?
       AND
       datetime(measurement_time,'unixepoch', 'localtime') < ?
   )
ORDER BY measurement_time DESC;
"""


def get_dataframe(conn: sqlite3.Connection,
                  device_name: str, from_date: str, exclude_to_date
                  ) -> DataFrame:
    params: Tuple = (device_name, from_date, exclude_to_date)
    try:
        read_df = pd.read_sql(_QUERY, conn, params=params, parse_dates=[COL_TIME])
        return read_df
    except Exception as err:
        raise err
