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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
public class PDPreProcess {
 

 

  
	public static class Map extends 
			Mapper<LongWritable, Text, IntWritable, AdjacencyList> {
	
		
		private IntWritable node = new IntWritable();
		private AdjacencyList list = new AdjacencyList(2);
		private BooleanWritable hasSelf = new BooleanWritable(false);


		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int src = Integer.valueOf(context.getConfiguration().get("source"));
			

			list = new AdjacencyList(2);
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			node.set(Integer.valueOf(tokenizer.nextToken()));
			list.add(Integer.valueOf(tokenizer.nextToken()),Integer.valueOf(tokenizer.nextToken()));
			context.write(node,list);
			//code for fault in our data
			if(!hasSelf.get() && ((list.getRawValue(0)) == src ||(key.get() == src)))
			{
				AdjacencyList loop = new AdjacencyList(2);
				hasSelf = new BooleanWritable(true);
				loop.add(src,0);
				context.write(new IntWritable(src),loop);

			}
			
			
			
			
		}
	}

	

	public static class Reduce extends 
			Reducer<IntWritable, AdjacencyList, IntWritable, AdjacencyList> {
		AdjacencyList finalList;
		public void reduce(IntWritable key, Iterable <AdjacencyList> Lists,
				Context context) throws IOException, InterruptedException {
			
			AdjacencyList finalList = new AdjacencyList(30);
			
			for (AdjacencyList list : Lists) {

					finalList.add(list);
					
			}
				
			context.write(key, finalList);
			
		}
	}


	public static void main(String[] args) throws Exception {
		// Run on a local node 
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");



		Job job = Job.getInstance(conf, "pdpreprocess");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(AdjacencyList.class);

		job.setJarByClass(PDPreProcess.class);

		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.waitForCompletion(true);
	}
}
