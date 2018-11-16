
import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BigramInitialCount {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
	{	
		private HashMap<String, Integer> bigramMap;
		private static final Integer MAX_MAP_SIZE = 1000;		
		private String prefix = "#";

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			HashMap<String, Integer> bigramMap = getHashMap();													
			String word = null;												
			
			String line = value.toString();
			Pattern pattern = Pattern.compile("[a-zA-Z]+");
			Matcher matcher = pattern.matcher(line);

			// Skip the first word
			if (prefix.equals("#") && matcher.find())
			{
				prefix = new String(matcher.group(0)); 														// read word
			}
			while (matcher.find())
			{
				word = new String (matcher.group(0)); 													// read the next word							

				String hashKey = new String(prefix.charAt(0) + " " + word.charAt(0));
				bigramMap.merge(hashKey, 1, Integer::sum);

				prefix = new String(word); 																// set new word as prefix for next iteration
			}
			flush(context, false);
		}

		public HashMap<String, Integer> getHashMap()
		{
			if (bigramMap == null)
				bigramMap = new HashMap<String, Integer>();
			return bigramMap;
		}		

		private void flush(Context context, boolean force) throws IOException, InterruptedException
		{
			HashMap<String, Integer> bigramMap = getHashMap();
			if (!force)
			{
				if (bigramMap.size() < MAX_MAP_SIZE)
					return;
			}
			Set<String> keySet = bigramMap.keySet();
			for (String hashKey : keySet)
			{
				context.write(new Text(hashKey), new IntWritable(bigramMap.get(hashKey)));
			}
			bigramMap.clear();
		}

		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			flush(context, true);
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class MyPartitioner extends Partitioner < Text, IntWritable >
	{
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks)
		{
			try {
				// Ensure that all keys with the same first letter reach the same reducer.
				char leftKey = key.toString().charAt(0);
				return String.valueOf(leftKey).hashCode() % numReduceTasks;
			} 
			catch (Exception e) {
				e.printStackTrace();
				return 0;
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", " ");

		String inputPath = args[0];
		String outputPath = args[1];

		Job job = Job.getInstance(conf, "bigraminitialcount");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setJarByClass(BigramInitialCount.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}
}
