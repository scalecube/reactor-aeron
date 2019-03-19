#!/usr/bin/env bash

#This script executes benchmarks from benchmarks.json

cd ../target

JAR_FILE=$(ls |grep jar)

render_template() {
eval "cat <<EOF
$(<$1)
EOF
" 2> /dev/null
}

TESTS_DATA=$(render_template ../scripts/benchmarks.json)

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

    echo "Finished $(_jq '.title')"

done

echo "All tests are passed"
