#!/bin/bash

# This script execute before 1_inst_libraries.sh execute and logout terminal.
#  before export my_passwd=xxxxx

# [usage] ./3_migrate_weatherdb.sh from-date 
#    from-date required (exmaple) 2022-01-01  
if [ -z "$1" ]; then
   echo "Required from-date (exmaple) 2022-01-01  !" 1>&2
   exit 1
fi

date +"%Y-%m-%d %H:%M:%S >Script START"
# Migrate SQLite3 weather.db (export csv) into PostgeSQL 
# scp from raspi-zero:SQLite3 weather.db 
scp pi@raspi-zero:~/db/weather.db ~/data/sql/weather/sqlite3db
exit1=$?
echo "scp paspi-zero:weather.db into sqlite3db directory >> status=$exit1"
if [ $exit1 -ne 0 ]; then
   echo "Fail scp paspi-zero:weather.db into sqlite3db directory!" 1>&2
   exit $exit1
fi

date +"%Y-%m-%d %H:%M:%S >weather.db copied"
cd ~/data/sql/weather

export PATH_WEATHER_DB=~/data/sql/weather/sqlite3db/weather.db
OUTPUT_PATH=~/data/sql/weather/csv
./getcsv_sqlite_device.sh "$OUTPUT_PATH"
./getcsv_sqlite_weather.sh "$1" "$OUTPUT_PATH"
exit1=$?
echo "export SQLite3 db to csv files. >> status=$exit1"
if [ $exit1 -ne 0 ]; then
   echo "Fail export SQLite3 db to weather csv files!"
   exit $exit1
fi

date +"%Y-%m-%d %H:%M:%S >CSV output complete"

cd ~/docker/postgres

docker-compose up -d
exit1=$?
echo "docker-compose up -d >> status=$exit1"
if [ $exit1 -ne 0 ]; then
   echo "Fail docker-compose up!" 1>&2
   exit $exit1
fi

# wait starting pg_ctl in container.
sleep 2

# 制約を外してインポート
date +"%Y-%m-%d %H:%M:%S >CSV importing..."
~/bin/dkr_import_weather.sh -o drop-constraint
exit1=$?
echo "Execute dkr_import_weather.sh >> status=$exit1"
date +"%Y-%m-%d %H:%M:%S >CSV import completed"

docker-compose down
date +"%Y-%m-%d %H:%M:%S >Script END"

cd ~

if [ $exit1 -ne 0 ]; then
   echo "Fail import all csv!" 1>&2
   exit $exit1
else
   echo "Database migration success."
   echo "rebooting."
   echo $my_passwd |sudo --stdin reboot
fi

