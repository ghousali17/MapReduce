import java.io.IOException;
import java.util.*;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PDNodeWritable implements Writable  
  {	private IntWritable nodeId; //list size is multiple of 2, i%2 = 0 for node id and i%2 for node distance.   
  	private IntWritable prevId;
    private IntWritable distance;
    private BooleanWritable visited;
    private AdjacencyList adjacencyList;


    public PDNodeWritable()
    {
      //System.out.println("Creating List!");
     this.nodeId = new IntWritable();
     this.prevId = new IntWritable(-1);
     this.distance = new IntWritable();
     this.visited = new BooleanWritable(false);
     this.adjacencyList = new AdjacencyList();
    }

    public void set(IntWritable nodeId, IntWritable distance, boolean visited, AdjacencyList adjacencyList)
    {
     this.nodeId = nodeId;
     this.distance = distance;
     this.prevId = new IntWritable(-1);
     this.visited = new BooleanWritable(visited);
     this.adjacencyList = adjacencyList;


    }
    public void set(IntWritable nodeId, int distance, boolean visited,AdjacencyList adjacencyList)
    {
     this.nodeId = nodeId;
     this.prevId = new IntWritable(-1);
     this.distance = new IntWritable(distance);
     this.visited = new BooleanWritable(visited);
     this.adjacencyList = adjacencyList;


    }
     public void set(PDNodeWritable node)
    {
     this.nodeId = node.nodeId;
     this.prevId = node.prevId;
     this.distance = node.distance;
     this.visited = node.visited;
     this.adjacencyList = node.adjacencyList;


    }
   
    public AdjacencyList getList()
    {
        return this.adjacencyList;
    }
    
    public void setList(AdjacencyList list)
    {
        this.adjacencyList = list;
    }
    

    public void setPath(int path)
    {
        this.prevId = new IntWritable(path);
    }

    public void setDistance(int distance)
    {
        this.distance = new IntWritable(distance);
    }
    public int getRawId()
    {
        return this.nodeId.get();
    }

    public IntWritable getId()
    {
        return this.nodeId;
    }
     public int getRawPath()
    {
        return this.prevId.get();
    }

    public IntWritable getPath()
    {
        return this.prevId;
    }
    public int getRawDistance()
    {
        return this.distance.get();
    }

    public IntWritable getDistance()
    {
        return this.distance;
    }

    public boolean isVisited()
    {
        return this.visited.get();
    }
    public void markVisited()
    {
        this.visited = new BooleanWritable(true);
    }
    @Override
     //Must overwrite to as writable is an abstract class.
    public void write(DataOutput out)  throws IOException{
    	this.nodeId.write(out);
        this.prevId.write(out);
        this.distance.write(out);
        this.visited.write(out);
        this.adjacencyList.write(out);

    	 }

      @Override
    //overriding default readFields method. 
    //It de-serializes the byte stream data
    //Must overwrite to as writable is an abstract class.
    public void readFields(DataInput in) throws IOException {
      	
      	this.nodeId.readFields(in);
        this.prevId.readFields(in);
        this.distance.readFields(in);
        this.visited.readFields(in);
        this.adjacencyList.readFields(in);
    }
    //if your output format is Text or if you use print for this type, the to string method will be invoked. 
    public String toString(){
        if(this.visited.get()){

            return nodeId.toString() + " "  + distance.toString() + " " + prevId.toString() + " visited " + adjacencyList.toString(); 
        }
            else{
                return nodeId.toString() + " " + distance.toString() + " " + prevId.toString() + " not visited " + adjacencyList.toString(); 
            }
    	
    }
}