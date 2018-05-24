#!/usr/bin/env bash
set -e

case ${DB} in

scylla)
    sudo curl -o /etc/apt/sources.list.d/scylla.list -L http://repositories.scylladb.com/scylla/repo/20fc70b18261bf832cf8e0733a27979c/ubuntu/scylladb-2.1-trusty.list
    sudo apt-get -qq update
    sudo apt-get install -y --allow-unauthenticated scylla-server
    sudo /usr/bin/scylla --options-file /etc/scylla/scylla.yaml ${SCYLLA_OPTS} &> /tmp/scylla.log &
    ;;

cassandra)
    sudo service cassandra start
    ;;

*)
    env
    false
    ;;

esac
