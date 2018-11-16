import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import java.io.IOException;
import java.util.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class FirstMap extends Mapper<IntWritable, AdjacencyList, IntWritable, MapOutput> { //first map reads adjacency list format and marks the source node
	public static enum ReachCounter { COUNT };
    private PDNodeWritable node = new PDNodeWritable();
	private IntWritable nodeId = new IntWritable();
    private IntWritable unreachable = new IntWritable(-1);
    private IntArrayWritable edge = new IntArrayWritable(2); //creates an array 0 -> previous node 1-> distance
		//private AdjacencyList list = new AdjacencyList();	

		public void map(IntWritable nodeId, AdjacencyList adjacencyList, Context context)
				throws IOException, InterruptedException {
   

   		int listSize = adjacencyList.getRawSize(); //get the size of the adjacency list
        int source = Integer.valueOf(context.getConfiguration().get("source")); //get source node from command line arguments
        System.out.println("Source:" + source);

        if(nodeId.get() == source)
        {
        node.set(nodeId,0, true, adjacencyList); //mark visited and distance 0 if the node is source. 
        
        context.write(nodeId, new MapOutput(node)); //pass on the graph structure
        //context.write(nodeId, new MapOutput(new IntWritable(0))); //logically distance to source should be zero 

        for(int i = 0; i < listSize; i+=2)
        { edge.set(0, nodeId);
          edge.set(1, adjacencyList.getValue(i+1));
          
          context.write(adjacencyList.getValue(i), new MapOutput(edge));
        }

        }
        else{

        node.set(nodeId,unreachable, false, adjacencyList); //mark visited and distance 0 if the node is source. 
        context.write(nodeId, new MapOutput(node)); //pass on the graph structure
        edge.set(0, nodeId);
        edge.set(1, unreachable);
        for(int i = 0; i < listSize; i+=2)
        {

          context.write(adjacencyList.getValue(i), new MapOutput(edge));
           
        }       } 


    
			
		}
	}