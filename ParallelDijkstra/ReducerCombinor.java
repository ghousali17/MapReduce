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

public  class ReducerCombinor extends 
      Reducer<IntWritable, MapOutput, IntWritable, MapOutput> {
      IntWritable unreachable = new IntWritable(-1);
      IntWritable distance = new IntWritable();
      PDNodeWritable node = new PDNodeWritable();
      IntArrayWritable path = new IntArrayWritable(2);
      



    public void reduce(IntWritable nodeId, Iterable <MapOutput> mapOutputs,
        Context context) throws IOException, InterruptedException {
        
        int minimumDistance = -1;
        int minimumPath = -1;
        int currentDistance; //placeholder for distance value
        node = null; 
      
        Iterator<MapOutput> mapOutput = mapOutputs.iterator();
          
        while(mapOutput.hasNext())
        {
          Writable rawValue = mapOutput.next().get();

          if(rawValue instanceof PDNodeWritable) //if you receive a node
          {
            if(node == null){
              node = new PDNodeWritable();
              node.set((PDNodeWritable)rawValue);


            }
            
              
              currentDistance = ((PDNodeWritable)rawValue).getRawDistance();
              if(minimumDistance == -1)
              {
                minimumDistance = currentDistance;
                minimumPath = ((PDNodeWritable)rawValue).getRawPath();

              }else if(currentDistance != -1 && currentDistance < minimumDistance){

                minimumDistance = currentDistance;
                minimumPath = ((PDNodeWritable)rawValue).getRawPath();
              }
            

          }
          else{ //received edge distance. 

                currentDistance = ((IntArrayWritable)rawValue).get(1).get();
                if(minimumDistance == -1)
                {
                  minimumDistance = currentDistance;
                  minimumPath = ((IntArrayWritable)rawValue).get(0).get();
                } else{
                  if(currentDistance != -1 && currentDistance < minimumDistance){
                    minimumDistance = currentDistance;
                    minimumPath = ((IntArrayWritable)rawValue).get(0).get();
                  }

                }

          }
          
        } //check all mapper result and write.
         if(node != null){
          node.setDistance(minimumDistance);
          node.setPath(minimumPath);
        //  System.out.println("Node:" + node);
          context.write(nodeId, new MapOutput(node));
         }
         else{

          path.set(0,new IntWritable(minimumPath));
          path.set(1,new IntWritable(minimumDistance)); 
          context.write(nodeId,new MapOutput(path));


         }
        

        
  
      
      
    }
  }
