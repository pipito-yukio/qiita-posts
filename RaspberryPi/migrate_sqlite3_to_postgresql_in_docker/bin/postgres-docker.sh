#!/bin/bash

docker_compose=$(which docker-compose)

case "$1" in
  start)
    echo "docker-directory: $2"
    cd $2 # Directory in docker-compose.yml
    $docker_compose up -d
    sleep 1
    systemd-notify --ready
    echo "PostgreSQL($2) container ready!"
    sleep 1
    container_ls=`docker container ls`
    echo "$container_ls"
    # After udp-weather-mon.service, other webapplication service
    cd ~
    ;;
  stop)
    # At shutdown
    cd $2
    $docker_compose down
    echo "PostgreSQL container down."
    cd ~
    ;;
  *)
    exit 1
    ;; 
esac

