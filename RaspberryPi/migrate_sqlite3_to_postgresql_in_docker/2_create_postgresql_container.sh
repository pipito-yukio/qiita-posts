#!/bin/bash

# Create PostgreSQL container with sensors_pgdb database and weather data tables.
date +"%Y-%m-%d %H:%M:%S >Script START"
cd ~/docker/postgres
docker-compose up --build -d
exit1=$?
echo "Docker create PostgreSQL container >> status=$exit1"
docker-compose down
if [ $exit1 -ne 0 ]; then
   echo "Fail docker create PostgreSQL container!" 1>&2
   exit $exit1
fi

cd ~/
date +"%Y-%m-%d %H:%M:%S >Script END"

echo "Done."

