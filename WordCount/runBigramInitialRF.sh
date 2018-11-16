#!/bin/bash

javac -classpath `yarn classpath` bigramInitialRF/BigramInitialRF.java -d bigramInitialRF/assets
jar -cvf bigramInitialRF/bigramInitialRF.jar -C bigramInitialRF/assets/ .
hadoop fs -rm -r /output || echo ""
hadoop jar bigramInitialRF/bigramInitialRF.jar BigramInitialRF /input /output 0
hadoop fs -cat /output/part-r-00000
