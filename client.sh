#!/bin/bash

javac -cp protobuf.jar:. Client/source/Client.java
java -cp protobuf.jar:. Client.bin.Client
