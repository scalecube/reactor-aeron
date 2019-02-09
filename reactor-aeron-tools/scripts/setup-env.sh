#!/usr/bin/env bash

declare -a addresses=("ip1" "ip2")

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
    scp -i $CERT_PATH $SRC_PATH/reactor-aeron-tools/target/*.jar $USER_NAME@$addr:/tmp/reactor-aeron/target/
    scp -i $CERT_PATH $SRC_PATH/reactor-aeron-tools/target/lib/* $USER_NAME@$addr:/tmp/reactor-aeron/target/lib/
    scp -i $CERT_PATH $SRC_PATH/reactor-aeron-tools/scripts/* $USER_NAME@$addr:/tmp/reactor-aeron/scripts/
done
