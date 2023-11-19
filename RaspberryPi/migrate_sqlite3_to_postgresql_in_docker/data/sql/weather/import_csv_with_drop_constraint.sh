#!/bin/bash

# 気象データCSVをテーブルにインポート
# PK制約をドロップ
cat<<-EOF | psql -Udeveloper sensors_pgdb 
ALTER TABLE weather.t_weather DROP CONSTRAINT pk_weather;
ALTER TABLE weather.t_weather DROP CONSTRAINT fk_device;
EOF

sleep 1

# データインポート
# t_device.csv into t_device table
# t_weather.csv into t_weather table
cat<<-EOF | psql -Udeveloper sensors_pgdb --echo-all
copy weather.t_device
 FROM '${1}/data/sql/weather/csv/t_device.csv' DELIMITER ',' CSV HEADER;
copy weather.t_weather
 FROM '${1}/data/sql/weather/csv/t_weather.csv' DELIMITER ',' CSV HEADER;
EOF

sleep 2

# PK制約を戻す
cat<<-EOF | psql -Udeveloper sensors_pgdb
ALTER TABLE weather.t_weather 
  ADD CONSTRAINT pk_weather PRIMARY KEY (did, measurement_time);
ALTER TABLE weather.t_weather 
  ADD CONSTRAINT fk_device FOREIGN KEY (did) REFERENCES weather.t_device (id);
EOF

