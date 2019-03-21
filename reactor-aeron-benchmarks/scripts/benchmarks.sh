#!/usr/bin/env bash

#This script executes benchmarks from benchmarks.json

TESTS_DATA=$(cat benchmarks.json)

for test in $(echo "${TESTS_DATA}" | jq -r '.[] | @base64'); do
    _jq() {
      echo ${test} | base64 --decode |jq -r ${1}
    }

    echo "Starting $(_jq '.title')"
    export TEST_VAR=help
    $(_jq '.server') > /dev/null 2>&1 &
    SERVER_PID=$! 

    $(_jq '.client') > /dev/null 2>&1 &
    CLIENT_PID=$!

    echo $SERVER_PID
    echo $CLIENT_PID

    sleep 200

    kill -9 -$(ps -o pgid= $SERVER_PID | grep -o [0-9]*)
    kill -9 -$(ps -o pgid= $CLIENT_PID | grep -o [0-9]*)

    echo "Finished $(_jq '.title')"

done

echo "All tests are passed"
