#!/usr/bin/env bash

cd $(dirname $0)
cd ../../../

JAR_FILE=$(ls target/reactor-aeron-benchmarks*.jar |grep jar)

java \
    -cp ${JAR_FILE}:target/lib/* \
    -XX:+UnlockDiagnosticVMOptions \
    -XX:GuaranteedSafepointInterval=300000 \
    -Daeron.threading.mode=SHARED \
    -Dagrona.disable.bounds.checks=true \
    -Dreactor.aeron.sample.idle.strategy=yielding \
    -Dreactor.aeron.sample.frameCountLimit=16384 \
    -Dreactor.aeron.sample.messageLength=16 \
    -Dreactor.aeron.sample.request=128 \
    -Daeron.mtu.length=16k \
    ${JVM_OPTS} reactor.aeron.rsocket.aeron.RSocketAeronPing
