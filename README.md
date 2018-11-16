# MapReduce

This repository is for my experiments with MapRedcuce in Hadoop 2.7.0. The repository is divded into different folders, each for a unique 
MapReduce program. 

## WordCount
 This contains code for calculating the word counts, the word pair counts (bigram count) and bigram relative frequencies.
 
 ## ParallelDijkstra
 This contains code for calculating all the possible shortest paths from a single source node. Dijkstra's algorithm is implemented as a 
 greedy Breadth First Search since we have worked under the assumption that the data might be too large for a single computer to process.
 
 
## Description
Parallel Dijkstra is parallel Breadth First Search program that finds the shortest paths to all possible nodes from a select vertice.
The input graph needs to have positive weights only. The input to the graph is a text file of the form:=

#### 1 2 7
#### 1 3 20
#### 2 3 3
#### 3 1 5
#### 4 1 9
#### 5 6 10

where each line represents an positive directed edge in the graph 1 2 7 means an edge from node 1 to node 2 with weight 7.

The Output of the porgram is a text file of shortest paths from the source node to all the possible node.
If We run ParallelDijkstra on the above input, with node 1 as source and set the number of iterations to 0 (iterate until convergance), the obtained result will be:

#### 1 0 nil
#### 2 7 1
#### 3 10 2

1 0 nil means the distance from source 1 to node 1 is 0 (node is same as source in this case) and the third word nil tells us their is
now previous node as we are at the source node. 2 7 1 meanas the distance from source 1 to node 2 is 7 and the previous node in the path
is 1. 

### PDPreProcess.java
The program converts the text input into a sequential Adjacency list format. The resulting file format is simply  (NodeID, AdjacencyList ).
A textual representation of the output from PDPreProcess.java is:

#### 1 (2 7), (3 20)
#### 2 (3 3)
#### 3 (1 5)
#### 4 (1 9)
#### 5 (6 10)


### PDNodeWritable.java
This is simply a user defined data type for MapReduce Framework. It shows how to write and serialise your own datatypes to make your life easier while coding in  hadoop.



### ParllelDijkstra.java
This is the main .java file for this project. It contains definition for our Mapper and Reducer Classes. It shows how to chain multiple
multiple MapReduce jobs together to achieve linearity in our programs.We have two types of Mappers and Reducers. The FirstMap.java converts the Adjaceny list in to nodes (PDNodeWritable) and passes it on to either Reduce.java or FinalReduce.java depending on the number of iterations required. Reduce .java takes either (nodeId,Distance) or (nodeId, Node) as an input, selects the minimum path and updates the node before writing it. Each MapReduce run expands our known frontier by one hops i.e. the distance of all the nodes within one hop of the currently known nodes are analysed and existing shortest paths are updated accordingly. 




 
 ## PageRank
 This contains code for calculating the page rank of some users on a social media site. All users are represented as nodes and their 
 connection as edges in a graph. 
