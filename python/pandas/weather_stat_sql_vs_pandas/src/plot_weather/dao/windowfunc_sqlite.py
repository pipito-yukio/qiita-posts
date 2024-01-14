import sqlite3
from dataclasses import dataclass
from typing import List, Tuple

"""
SQLite3 気象データベースから外気温統計情報を取得するモジュール
SQL window function with CTE
"""


@dataclass
class TempOut:
    appear_time: str
    temper: float


_STAT_QUERY = """
WITH find_records AS (
SELECT
  datetime(measurement_time, 'unixepoch', 'localtime') AS measurement_time,
  CASE
    WHEN temp_out <= min(temp_out) OVER (PARTITION BY date(measurement_time))
	THEN temp_out
  END AS min_temp_out,
  CASE
    WHEN temp_out >= max(temp_out) OVER (PARTITION BY date(measurement_time))
	THEN temp_out
  END AS max_temp_out
FROM
  t_weather tw INNER JOIN t_device td ON tw.did = td.id
WHERE
  td.name = ?
  AND (
    datetime(measurement_time, 'unixepoch', 'localtime') >= ? 
    AND 
    datetime(measurement_time, 'unixepoch', 'localtime') < ?
  )
-- 降順でソート ※最低気温と最高気温は指定範囲に複数出現するが、直近レコードを取得値とする  
ORDER BY measurement_time DESC
), min_temp_out_records AS (
--直近の最低気温 1レコード
  SELECT
     measurement_time, min_temp_out AS temp_out
  FROM
     find_records
  WHERE
     min_temp_out IS NOT NULL LIMIT 1
), max_temp_out_records AS (
--直近の最高気温 1レコード
  SELECT
     measurement_time, max_temp_out AS temp_out
  FROM
     find_records
  WHERE
     max_temp_out IS NOT NULL LIMIT 1
)
SELECT * FROM min_temp_out_records
UNION ALL
SELECT * FROM max_temp_out_records
"""


def get_temp_out_stat(conn: sqlite3.Connection,
                      device_name: str,
                      from_date: str, exclude_to_date: str) -> List[TempOut]:
    params: Tuple[str, str, str] = (device_name, from_date, exclude_to_date,)
    result: List[TempOut] = []
    with conn:
        cursor: sqlite3.Cursor = conn.execute(_STAT_QUERY, params)
        rows: List[Tuple[str, float]] = cursor.fetchall()
        if len(rows) > 0:
            for row in rows:
                measurement_time: str = row[0]
                temp_out: float = row[1]
                rec: TempOut = TempOut(measurement_time, temp_out)
                result.append(rec)
    return result
