#!/bin/bash

rmiregistry &
javac -cp protobuf.jar:. Datanode/source/DataNode.java -d Datanode/bin
java -cp protobuf.jar:. Datanode.bin.DataNode
