#!/usr/bin/env bash

# possible environment variables:
# $TRACES_FOLDER : folder where traces files are generated
# $CHARTS_FOLDER : folder where chart reports is created
# $CHART_TEMPLATE : based on which chart is created

cd $(dirname $0)
cd ../

if [ ! -f ./target/trace-reporter.jar ]; then
	echo "trace-reporter.jar File not found! ... downloading...."
	LATEST_VERSION_JAR=https://github.com/scalecube/trace-reporter/blob/latest/release/trace-reporter-jar-with-dependencies.jar?raw=true
    wget ${LATEST_VERSION_JAR} -O ./target/trace-reporter.jar
fi


FOLDER_LATENCY=./target/traces/reports/latency/
FOLDER_THROUGHPUT=./target/traces/reports/throughput/
FOLDER_OUTPUT=./target/traces/reports/charts/

java \
    -jar ./target/trace-reporter.jar -i ${FOLDER_LATENCY} -o ${FOLDER_OUTPUT} -t ./src/main/resources/latency-report.json
    
java \
    -jar ./target/trace-reporter.jar -i ${FOLDER_THROUGHPUT} -o ${FOLDER_OUTPUT} -t ./src/main/resources/throughput-report.json    