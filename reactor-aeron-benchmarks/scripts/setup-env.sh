#!/usr/bin/env bash

declare -a addresses=("ip1", "ip2")

CERT_PATH=
SRC_PATH=
USER_NAME=

cd $SRC_PATH

mvn clean install -DskipTests

for addr in ${addresses[@]}
do
    echo "####### Setting up for: #######"
    echo "$addr"
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH $USER_NAME@$addr 'sudo rm -rf /tmp/*'
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH $USER_NAME@$addr 'mkdir -p /tmp/reactor-aeron/scripts/'
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH $USER_NAME@$addr 'mkdir -p /tmp/reactor-aeron/target/lib'
    scp -r -i $CERT_PATH $SRC_PATH/reactor-aeron-benchmarks/target/*.jar $USER_NAME@$addr:/tmp/reactor-aeron/target/
    scp -r -i $CERT_PATH $SRC_PATH/reactor-aeron-benchmarks/target/lib/* $USER_NAME@$addr:/tmp/reactor-aeron/target/lib/
    scp -r -i $CERT_PATH $SRC_PATH/reactor-aeron-benchmarks/scripts/* $USER_NAME@$addr:/tmp/reactor-aeron/scripts/
done
