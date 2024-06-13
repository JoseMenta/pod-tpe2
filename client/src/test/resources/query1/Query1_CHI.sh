#!/bin/bash

chmod u+x client/src/test/resources/testInit.sh

./client/src/test/resources/testInit.sh "$@"

cd server/target/tpe2-g6-server-1.0-SNAPSHOT
chmod u+x run-server.sh

./run-server.sh > /dev/null 2>&1 & server_pid=$!

sleep 1

cd ../../../client/target/tpe2-g6-client-1.0-SNAPSHOT/

chmod u+x query1.sh

./query1.sh -Daddresses="127.0.0.1" -Dcity=CHI  -DinPath="C:\Users\lauti\Documents\pod-tpe2\client\src\test\resources\query1\inpath" -DoutPath="C:\Users\lauti\Documents\pod-tpe2\client\src\test\resources\query1\outpath" > /dev/null

diff -q outpath/expected.csv outpath/result.csv

pkill -P "$server_pid"