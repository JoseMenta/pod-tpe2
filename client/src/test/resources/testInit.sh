#!/bin/bash

if [[ '-b' == "$1" ]]; then
    echo "Building the project"

    mvn clean package > /dev/null

    cd server/target

    tar -xzf tpe2-g6-server-1.0-SNAPSHOT-bin.tar.gz

    cd ../../client/target/

    tar -xzf tpe2-g6-client-1.0-SNAPSHOT-bin.tar.gz
else
    cd client/target/

    echo "Project was not built"
fi

cd tpe2-g6-client-1.0-SNAPSHOT