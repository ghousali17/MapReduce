
import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.regex.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BigramInitialRF 
{
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		private static final IntWritable V = new IntWritable();
		private static final Text K = new Text();
		private HashMap<String, Integer> bigramMap;
		private static final Integer MAX_MAP_SIZE = 1000;		
		private static String prefix = "#";

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			HashMap<String, Integer> bigramMap = getHashMap();													
			String word = null;														// first letter of suffix
			
			String line = value.toString();
			Pattern pattern = Pattern.compile("[a-zA-Z]+");
			Matcher matcher = pattern.matcher(line);

			// Skip the first word
			if( prefix.toString().equals("#") && matcher.find())
			{
				prefix = new String(matcher.group(0)); 							// set initial for word
			}
			while (matcher.find())
			{
				word = new String (matcher.group(0)); 													// read the next word							

				String hashKey = new String(prefix.charAt(0) + " " + word.charAt(0));
				
				bigramMap.merge(hashKey, 1, Integer::sum);
				bigramMap.merge(hashKey.charAt(0)+ " *", 1, Integer::sum);

				prefix = new String(word);									
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
				K.set(hashKey);
				V.set(bigramMap.get(hashKey));
				context.write(K, V);
			}
			bigramMap.clear();
		}

		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			flush(context, true);
		}
	}

	protected static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		private static final IntWritable count = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			count.set(sum);
			context.write(key, count);
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> 
	{
		private static int totalCount;
		private static double theta;
		private static final Text V = new Text();
		private static final DecimalFormat df = new DecimalFormat("#.########");

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			df.setRoundingMode(RoundingMode.CEILING);
			Configuration conf = context.getConfiguration();
			theta = Double.parseDouble(conf.get("theta"));

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			String rightKey = String.valueOf(key.toString().charAt(2));
			if (rightKey.equals("*")) {
				totalCount = sum;
			}
			else {
				double rf = (double)sum / (double)totalCount;
				if (!(rf < theta)) {
					V.set(df.format(rf));
					context.write(key, V);
				}
			}
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
		conf.set("theta", args[2]);
		conf.set("mapreduce.output.textoutputformat.separator", " ");

		Job job = Job.getInstance(conf, "bigraminitialrf");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class);

		job.setJarByClass(BigramInitialRF.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(MyPartitioner.class);

		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class); 

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}
}
