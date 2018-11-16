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

public class IntArrayWritable implements Writable  
  { private List<IntWritable> listData; //list size is multiple of 2, i%2 = 0 for node id and i%2 for node distance.   
    private IntWritable size;

    public IntArrayWritable()
    {
      //System.out.println("Creating List!");
      this.listData = new ArrayList <IntWritable>();
      this.size = new IntWritable(0); 
    }

    public IntArrayWritable(int capacity)
    {
      //System.out.println("Creating List!");
      this.listData = new ArrayList <IntWritable>(capacity);
      this.size = new IntWritable(capacity);
      for(int i = 0; i < capacity; i++)
      {
        listData.add(new IntWritable(0));
      } 
    }

    public IntWritable get(int index){
        return listData.get(index);
    }
    public void set( int index, IntWritable val1 )
    {
        listData.set(index,val1);
        
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
        //  System.out.println("Mapper writing:" +this.listData.get(i).get() + " " + this.listData.get(i+1).get());
        }

    //  toReturn += toReturn += "(" + listData.get(size-2) + " " +  listData.get(size-1) + ")";
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
        //  System.out.println("Reducer reading:" + nodeId.get() + " " + distance.get());
        }
    }
    //if your output format is Text or if you use print for this type, the to string method will be invoked. 
    public String toString(){
        
       
        if(this.size.get() <2)
            return "";
        if(listData.get(0).get() == -1)
        {
            return listData.get(1) + " nil";
        }
        else{
            return listData.get(1) + " " + listData.get(0);
        }   
    
        
    }
}