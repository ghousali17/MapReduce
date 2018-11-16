#!/bin/bash

javac -classpath `yarn classpath` wordLengthCount/WordLengthCount.java -d wordLengthCount/assets
jar -cvf wordLengthCount/wordLengthCount.jar -C wordLengthCount/assets/ .
hadoop fs -rm -r /output || echo ""
hadoop jar wordLengthCount/wordLengthCount.jar WordLengthCount /input /output
hadoop fs -cat /output/part-r-00000