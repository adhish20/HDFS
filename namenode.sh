#!/bin/bash

rmiregistry &
javac -cp protobuf.jar:. Namenode/source/NameNode.java -d Namenode/bin
java -cp protobuf.jar:. Namenode.bin.NameNode
