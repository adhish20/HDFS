#!/bin/bash

javac -cp protobuf.jar:. Client/source/Client.java -d Client/bin
java -cp protobuf.jar:. Client.bin.Client
