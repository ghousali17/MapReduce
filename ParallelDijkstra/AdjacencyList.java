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

public class AdjacencyList implements Writable  
  {	private List<IntWritable> listData; //list size is multiple of 2, i%2 = 0 for node id and i%2 for node distance.   
  	private IntWritable size;

    public AdjacencyList()
    {
      //System.out.println("Creating List!");
      this.listData = new ArrayList <IntWritable>();
      this.size = new IntWritable(0); 
    }

    public AdjacencyList(int capacity)
    {
      //System.out.println("Creating List!");
      this.listData = new ArrayList <IntWritable>(capacity);
      this.size = new IntWritable(0); 
    }


    public int getRawSize(){
    	return this.size.get();
    }

    public IntWritable getSize(){
    	return this.size;
    }

    public int getRawValue(int index){
        return listData.get(index).get();
    }
    public IntWritable getValue(int index){
    	return listData.get(index);
    }
    public void add(int nodeid, int distance )
    {
    	listData.add(new IntWritable(nodeid));
    	listData.add(new IntWritable(distance));
    	size.set(size.get()+2);
    
    }
    public void add(IntWritable nodeid, IntWritable distance )
    {
    	listData.add(nodeid);
    	listData.add(distance);
    	size.set(size.get()+2);
    
    }

    public void add(AdjacencyList list)
    {   int argSize = list.getRawSize();
    	
    	size.set(size.get()+argSize);
    	
    	for(int i = 0; i < argSize; i++)
    	{
    		listData.add(list.getValue(i));
    	}
    	
    
    }
    
    @Override
     //Must overwrite to as writable is an abstract class.
    public void write(DataOutput out)  throws IOException{
    	

    	 
    	int size = this.size.get();
        this.size.write(out);
        for(int i = 0; i < size; i+=2 )
    	{

    		this.listData.get(i).write(out);
    		this.listData.get(i+1).write(out);
    	//	System.out.println("Mapper writing:" +this.listData.get(i).get() + " " + this.listData.get(i+1).get());
    	}

    //	toReturn +=	toReturn += "(" + listData.get(size-2) + " " +  listData.get(size-1) + ")";
    }

      @Override
    //overriding default readFields method. 
    //It de-serializes the byte stream data
    //Must overwrite to as writable is an abstract class.
    public void readFields(DataInput in) throws IOException {
      	
      	this.listData = new ArrayList <IntWritable>();
      	this.size = new IntWritable(0); 
        this.size.readFields(in);

        int size = this.size.get();
        for(int i = 0; i < size; i+=2 )
    	{    
    		IntWritable nodeId = new IntWritable();
    	    IntWritable distance = new IntWritable();
    		nodeId.readFields(in);
    		distance.readFields(in);
    		this.listData.add(nodeId);
    		this.listData.add(distance);
    	//	System.out.println("Reducer reading:" + nodeId.get() + " " + distance.get());
    	}
    }
    //if your output format is Text or if you use print for this type, the to string method will be invoked. 
    public String toString(){
    	int size = this.size.get();
    	String toReturn = "";
    	for(int i = 0; i < size-2; i+=2 )
    	{

    		toReturn += "(" + listData.get(i) + " " +  listData.get(i+1) + "), ";

    	}
        if(size >= 2){
            toReturn += "(" + listData.get(size-2) + " " +  listData.get(size-1) + ")";
        }
    
    	return toReturn; 
    }
}