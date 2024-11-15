#!/bin/bash

docker_cmd=$(which docker)

case "$1" in
  start)
    echo "docker-directory: $2"
    cd $2 # Directory in docker-compose.yml
    $docker_cmd compose up -d
    sleep 1
    systemd-notify --ready
    echo "PostgreSQL($2) container ready!"
    sleep 1
    container_ls=$(${docker_cmd} container ls)
    echo "$container_ls"
    cd ~
    ;;
  stop)
    # At shutdown
    exists_container=$(${docker_cmd} container ls | grep postgres-raspi)
    if [ -n "$exists_container" ]; then
       cd $2
       $docker_cmd compose down
       echo "PostgreSQL($2) container down!"
       cd ~
    else
       echo "docker.service already shutdownded."
    fi
    ;;
  *)
    exit 1
    ;; 
esac

