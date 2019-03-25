#!/usr/bin/env bash

#This script executes benchmarks from benchmarks.json

TESTS_DATA=$(cat benchmarks.json)

for test in $(echo "${TESTS_DATA}" | jq -r '.[] | @base64'); do
    _jq() {
      echo ${test} | base64 --decode |jq -r ${1}
    }

    echo "Starting $(_jq '.title')"

    # Add JVM_OPTS to tests if they exist
    export JVM_OPTS=$(_jq '.JVM_OPTS')
    if [ "$JVM_OPTS" == null ]
    then
        unset JVM_OPTS      
    fi    

    $(_jq '.server') > /dev/null 2>&1 &
    SERVER_PID=$! 

    $(_jq '.client') > /dev/null 2>&1 &
    CLIENT_PID=$!

    echo $SERVER_PID
    echo $CLIENT_PID

    sleep 10

    # kill test processes with their childs
    pkill -TERM -P $SERVER_PID
    pkill -TERM -P $CLIENT_PID

    echo "Finished $(_jq '.title')"

done

echo "All tests are passed"
