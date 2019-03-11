#!/usr/bin/env bash

cd $(dirname $0)
cd ../../

JAR_FILE=$(ls target |grep jar)

java \
    -cp target/${JAR_FILE}:target/lib/* \
    -XX:BiasedLockingStartupDelay=0 \
    -Djava.net.preferIPv4Stack=true \
    -Daeron.term.buffer.sparse.file=false \
    -Daeron.threading.mode=SHARED \
    -Dagrona.disable.bounds.checks=true \
    -Dreactor.aeron.sample.embeddedMediaDriver=true \
    -Dreactor.aeron.sample.exclusive.publications=true \
    -Dreactor.aeron.sample.idle.strategy=yielding \
    -Dreactor.aeron.sample.frameCountLimit=16384 \
    -Dreactor.aeron.sample.messageLength=1024 \
    -Daeron.mtu.length=16k \
    ${JVM_OPTS} reactor.aeron.pure.ClientThroughput
