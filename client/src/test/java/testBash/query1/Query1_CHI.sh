#!/bin/bash

chmod u+x client/src/test/resources/testInit.sh

./client/src/test/resources/testInit.sh "$@"

cd server/target/tpe1-g6-server-1.0-SNAPSHOT
chmod u+x run-server.sh

./run-server.sh > /dev/null 2>&1 & server_pid=$!

sleep 1

cd ../../../client/target/tpe1-g6-client-1.0-SNAPSHOT/

chmod u+x query1.sh

./query1.sh -Daddress=localhost -Dcity=CHI  -DinPath=../../src/test/testBash/query1/inpath -DoutPath=../../src/test/testBash/query1/outpath > /dev/null

diff -q outpath/expected.csv outpath/result.csv

pkill -P "$server_pid"