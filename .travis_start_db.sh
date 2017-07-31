#!/usr/bin/env bash
set -e

case ${DB} in

scylla)
    echo "deb [arch=amd64] http://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu trusty scylladb-1.7/multiverse" | sudo tee -a /etc/apt/sources.list > /dev/null
    sudo apt-get -qq update
    sudo apt-get install -y --allow-unauthenticated scylla-server
    sudo /usr/bin/scylla --options-file /etc/scylla/scylla.yaml ${SCYLLA_OPTS} ${SCYLLA_OPTS_LOG} &
    ;;

cassandra)
    sudo service cassandra start
    ;;

*)
    env
    false
    ;;

esac
