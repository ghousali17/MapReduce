#!/bin/bash

javac -classpath `yarn classpath` bigramInitialCount/BigramInitialCount.java -d bigramInitialCount/assets
jar -cvf bigramInitialCount/bigramInitialCount.jar -C bigramInitialCount/assets/ .
hadoop fs -rm -r /output || echo ""
hadoop jar bigramInitialCount/bigramInitialCount.jar BigramInitialCount /input /output
hadoop fs -cat /output/part-r-00000