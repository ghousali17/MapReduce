all: pagerank
	
pagerank: PageRank.java PRAdjust.java PRNodeWritable.java PRPreProcess.java
	test -d ./src || mkdir ./src
	javac -classpath `yarn classpath` PRPreProcess.java PRNodeWritable.java PRAdjust.java PageRank.java -d src
	jar -cvf pagerank.jar -C src .

clean:
	rm *.jar || echo ""
	rm -r ./src || echo ""
