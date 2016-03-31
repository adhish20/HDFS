#!/bin/bash

rmiregistry &
javac -cp protobuf.jar:. Datanode/source/DataNode.java
java -cp protobuf.jar:. Datanode.bin.DataNode
