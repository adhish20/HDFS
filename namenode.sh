#!/bin/bash

rmiregistry &
javac -cp protobuf.jar:. Namenode/source/NameNode.java
java -cp protobuf.jar:. Namenode.bin.NameNode
