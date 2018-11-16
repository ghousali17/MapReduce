#!/bin/bash

javac -classpath `yarn classpath` wordCount/WordCount.java -d wordCount/assets
jar -cvf wordCount/wordCount.jar -C wordCount/assets/ .
hadoop fs -rm -r /output || echo ""
hadoop jar wordCount/wordCount.jar WordCount /input /output
hadoop fs -cat /output/part-r-00000