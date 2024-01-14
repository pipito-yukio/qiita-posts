#!/bin/bash

next_to_date() {
    retval=$(date -d "$1 1 days" +'%F');
    echo "$retval"
}

get_records() {
  device_name="$1";
  from_date="$2";
  eclude_to_date="$3"
cat<<-EOF | sqlite3 "${PATH_WEATHER_DB}" -csv
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
  td.name = '${device_name}'
  AND (
    datetime(measurement_time, 'unixepoch', 'localtime') >= '${from_date}' 
    AND 
    datetime(measurement_time, 'unixepoch', 'localtime') < '${eclude_to_date}'
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
SELECT * FROM max_temp_out_records;
EOF
}

# Eclude to_date
exclude_to_date=$(next_to_date "$2");

echo '"measurement_time","temp_out"'
get_records "$1" "$2" "${exclude_to_date}"

