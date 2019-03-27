#!/usr/bin/env bash

#This script executes benchmarks from benchmarks.json

TESTS_DATA=$(cat benchmarks.json)

for test in $(echo "${TESTS_DATA}" | jq -r '.[] | @base64'); do
    _jq() {
      echo ${test} | base64 --decode |jq -r ${1}
    }

    echo "Starting $(_jq '.title')"
	
	if [ ! "$(_jq '.title')" == null ]
	then
		export JVM_OPTS="-Dreactor.aeron.report.name=$(_jq '.title')"
	fi
	
    # Add JVM_OPTS to tests if they exist
    if [ ! "$(_jq '.args')" == null ]
    then
        for row in $(_jq '.args[]'); do
            JVM_OPTS+=" -D$row"
        done
    fi
    
    echo $JVM_OPTS
    
    $(_jq '.server') > /dev/null 2>&1 &
    SERVER_PID=$! 

    $(_jq '.client') > /dev/null 2>&1 &
    CLIENT_PID=$!

    sleep 75

    # kill test processes with their childs
    pkill -TERM -P $SERVER_PID
    pkill -TERM -P $CLIENT_PID

    echo Finished $(_jq '.title')

done

echo "All tests are passed"