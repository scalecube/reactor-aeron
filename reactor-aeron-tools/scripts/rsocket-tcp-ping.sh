#!/usr/bin/env bash

SCRIPTS_DIR=$(dirname $0)
TARGET_DIR="${SCRIPTS_DIR%/*}"/target
JAR_FILE=$(ls ${TARGET_DIR} |grep jar)

${JAVA_HOME}/bin/java \
    -cp ${TARGET_DIR}/${JAR_FILE}:${TARGET_DIR}/lib/* \
    -XX:+UnlockDiagnosticVMOptions \
    -XX:GuaranteedSafepointInterval=300000 \
    -Dreactor.aeron.sample.messages=100000000 \
    -Dreactor.aeron.sample.messageLength=64 \
    -Dreactor.aeron.sample.report.delay=2 \
    -Dreactor.aeron.sample.report.interval=2 \
    ${JVM_OPTS} reactor.aeron.demo.rsocket.RsocketTcpPing
