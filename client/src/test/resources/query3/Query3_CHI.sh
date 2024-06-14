#!/bin/bash

chmod u+x client/src/test/resources/testInit.sh

./client/src/test/resources/testInit.sh "$@"

cd server/target/tpe2-g6-server-1.0-SNAPSHOT
chmod u+x run-server.sh

./run-server.sh > /dev/null 2>&1 & server_pid=$!

sleep 1

cd ../../../client/target/tpe2-g6-client-1.0-SNAPSHOT/

chmod u+x query3.sh

./query3.sh -Daddresses="127.0.0.1" -Dcity=CHI  -DinPath="../../src/test/resources/inpath" -DoutPath="../../src/test/resources/query3/outpath" -Dn=5 > /dev/null

diff ../../src/test/resources/query3/outpath/ExpectedResult.csv ../../src/test/resources/query3/outpath/query3.csv

pkill -P "$server_pid"