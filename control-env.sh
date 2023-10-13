#!/usr/bin/env bash

function stop {
  echo "Stopping and removing containers"
  docker compose --project-name etlstocks down
}

function cleanup {
  echo "Removing volume"
  docker volume rm etlstocks_postgres-data
  docker volume rm etlstocks_superset
}

function start {
  echo "Starting up"
  docker compose --project-name etlstocks up -d
}

function update {
  echo "Updating code ..."
  git pull --all

  echo "Updating docker images ..."
  docker compose --project-name etlstocks pull

  echo "You probably should restart"
}

function info {
  echo '
  Everything is ready, access your host to learn more (ie: http://localhost/)
  '
}

function superset-init {
  echo 'Initializing Superset database using postgres'
  docker exec -it superset superset-init
}

function superset-start {
  echo 'Starting Superset container'
  docker container start superset
}

function superset-stop {
  echo 'Stopping Superset container'
  docker container stop superset
}

function psql {
  docker exec -it postgres psql -U workshop workshop
}

case $1 in
  start )
  start
  info
    ;;

  stop )
  stop
    ;;

  cleanup )
  stop
  cleanup
    ;;

  update )
  update
    ;;

  logs )
  docker compose --project-name etlstocks logs -f
    ;;

  superset-start )
  superset-start
    ;;
  
  superset-stop )
  superset-stop
    ;;
  
  superset-init )
  superset-init
    ;;
  psql )
  psql
    ;;

  * )
  printf "ERROR: Missing command\n  Usage: `basename $0` (start|stop|cleanuplogs|update|superset-start|superset-stop)\n"
  exit 1
    ;;
esac
