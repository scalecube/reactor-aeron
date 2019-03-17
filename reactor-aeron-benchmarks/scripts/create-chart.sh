#!/usr/bin/env bash

# possible environment variables:
# $TRACES_FOLDER : folder where traces files are generated
# $CHARTS_FOLDER : folder where chart reports is created
# $CHART_TEMPLATE : based on which chart is created

LATEST_VERSION_JAR=https://github.com/scalecube/trace-reporter/blob/latest/release/trace-reporter-jar-with-dependencies.jar?raw=true

cd $(dirname $0)
cd ../


if [ ! -f ./target/trace-reporter.jar ]; then
    echo "trace-reporter File not found! ... downloading...."
    wget ${LATEST_VERSION_JAR} -O ./target/trace-reporter.jar
fi

java \
    -jar ./target/trace-reporter.jar -i ./target/traces/latency -o ./target/charts -t ./src/main/resources/latency-report.json
    
java \
    -jar ./target/trace-reporter.jar -i ./target/traces/throughput -o ./target/charts -t ./src/main/resources/throughput-report.json    