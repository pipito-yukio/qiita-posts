#!/bin/bash

docker_cmd=$(which docker)

# Create PostgreSQL container with qiita_exampledb database.
cd ~/docker/postgres
$docker_cmd compose up --build -d
exit1=$?
echo "Docker create PostgreSQL container >> status=$exit1"
$docker_cmd compose down
if [ $exit1 -ne 0 ]; then
   exit $exit1
fi

cd ~/
echo "Done, sudo reboot now."

