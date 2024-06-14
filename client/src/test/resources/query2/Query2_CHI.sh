#!/bin/bash

chmod u+x client/src/test/resources/testInit.sh

./client/src/test/resources/testInit.sh "$@"

cd server/target/tpe2-g6-server-1.0-SNAPSHOT
chmod u+x run-server.sh

./run-server.sh > /dev/null 2>&1 & server_pid=$!

sleep 1

cd ../../../client/target/tpe2-g6-client-1.0-SNAPSHOT/

chmod u+x query2.sh

./query2.sh -Daddresses="127.0.0.1" -Dcity=CHI  -DinPath="../../src/test/resources/inpath" -DoutPath="../../src/test/resources/query2/outpath" > /dev/null

diff ../../src/test/resources/query2/outpath/ExpectedResult.csv ../../src/test/resources/query2/outpath/query2.csv

pkill -P "$server_pid"