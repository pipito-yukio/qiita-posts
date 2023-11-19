#!/bin/bash

get_csv() {
cat<<-EOF | sqlite3 "$PATH_WEATHER_DB" -csv
    SELECT id, name, name FROM t_device ORDER BY id;
EOF
}

# init option value
csv_filepath="${1}/t_device.csv"

header='"id","name","description"'
echo $header > "${csv_filepath}"
get_csv >> "${csv_filepath}"
if [ $? = 0 ]; then
   echo "Output device csv to ${csv_filepath}"
   row_count=$(cat "${csv_filepath}" | wc -l)
   row_count=$(( row_count - 1))
   echo "Record count: ${row_count}" 
else
   echo "Output error" 1>&2
fi   

