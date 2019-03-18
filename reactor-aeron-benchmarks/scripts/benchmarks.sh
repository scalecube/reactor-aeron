#!/bin/bash

TESTS_DATA=$(cat benchmarks.json)
cd ../..



JAR_FILE=$(ls target |grep jar)

for test in $(echo "${TESTS_DATA}" | jq -r '.[] | @base64'); do
    _jq() {
      echo ${test} | base64 --decode |jq -r ${1}
    }

    echo "Starting $(_jq '.title')"

    $(_jq '.server')  > /dev/null 2>&1 &
    SERVER_PID=$! 

    $(_jq '.client') > /dev/null 2>&1 &
    CLIENT_PID=$!

    sleep 2m

    kill -9 $CLIENT_PID $SERVER_PID

done
