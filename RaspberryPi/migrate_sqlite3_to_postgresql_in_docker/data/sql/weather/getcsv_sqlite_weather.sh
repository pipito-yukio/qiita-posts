#!/bin/bash

# CSV形式の出力: get_csv "${where}"
get_csv() {
    where="$1";
cat<<-EOF | sqlite3 "$PATH_WEATHER_DB" -csv
    SELECT
      did,
      datetime(measurement_time, 'unixepoch', 'localtime'), 
      temp_out, temp_in, humid, pressure
    FROM
      t_weather
    WHERE
      ${where}
    ORDER BY did, measurement_time;
EOF
}

# getcsv_sqlite_weather.sh 2021-01-01 ~/Downloads/csv
# All parameter required
where="(datetime(measurement_time, 'unixepoch', 'localtime') >= '"${1}"')"
csv_filepath="${2}/t_weather.csv"

header='"did","measurement_time","temp_out","temp_in","humid","pressure"'
echo $header > "${csv_filepath}"
get_csv "${where}" >> "${csv_filepath}"
if [ $? = 0 ]; then
   echo "Output t_weather csv to ${csv_filepath}"
   row_count=$(cat "${csv_filepath}" | wc -l)
   row_count=$(( row_count - 1))
   echo "Record count: ${row_count}" 
else
   echo "Output error"
fi

