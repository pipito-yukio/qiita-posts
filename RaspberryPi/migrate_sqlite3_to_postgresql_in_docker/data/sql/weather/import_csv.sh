#!/bin/bash

# 気象データCSVをテーブルにインポート
cat<<-EOF | psql -Udeveloper sensors_pgdb --echo-all
copy weather.t_device
 FROM '${1}/data/sql/weather/csv/t_device.csv' DELIMITER ',' CSV HEADER;
copy weather.t_weather
 FROM '${1}/data/sql/weather/csv/t_weather.csv' DELIMITER ',' CSV HEADER;
EOF

