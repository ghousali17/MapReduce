all: dijkstra


dijkstra: ParallelDijkstra.java IntArrayWritable.java AdjacencyList.java  ReducerCombinor.java FinalReduce.java MapOutput.java PDNodeWritable.java PDPreProcess.java
	test -d ./src || mkdir ./src
	javac -classpath `yarn classpath` ParallelDijkstra.java IntArrayWritable.java AdjacencyList.java  ReducerCombinor.java FinalReduce.java MapOutput.java PDNodeWritable.java PDPreProcess.java -d src
	jar -cvf dijkstra.jar -C src .

clean:
	rm *.jar || echo ""
	rm -r ./src || echo ""
