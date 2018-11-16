// Reference:
// https://vangjee.wordpress.com/2012/03/07/the-in-mapper-combining-design-pattern-for-mapreduce-programming/

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordLengthCount 
{
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> 
	{
		private HashMap<Integer, Integer> hmap;
		private static final Integer MAX_MAP_SIZE = 1000;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{			
			HashMap<Integer, Integer> hmap = getHashMap();

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				Integer hashKey = tokenizer.nextToken().length();
				// If no hashKey, put 1, else put sum(value, 1)
				hmap.merge(hashKey, 1, Integer::sum);
			}

			flush(context, false);
		}

		public HashMap<Integer, Integer> getHashMap()
		{
			// This method allows us to maintain a single Hashmap for 
			// evey Map object. In this way, each map method call of
			// this object will share one Hashmap.
			if (hmap == null) 
			{
				hmap = new HashMap<Integer, Integer>();
			}
			return hmap;
		}

		private void flush(Context context, boolean force) throws IOException, InterruptedException
		{
			// Emit contents of hashmap if forced or size > MAX
			HashMap<Integer, Integer> hmap = getHashMap();
			if (!force) 
			{
				if (hmap.size() < MAX_MAP_SIZE)
					return;
			}
			Set<Integer> keySet = hmap.keySet();
			for (Integer hashKey : keySet) 
			{
				context.write(new IntWritable(hashKey), new IntWritable(hmap.get(hashKey)));
			}
			hmap.clear();
		}

		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			flush(context, true);	// Force flush
		}		
	}

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
	{
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", " ");

		String inputPath = args[0];
		String outputPath = args[1];

		Job job = Job.getInstance(conf, "wordlengthcount");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setJarByClass(WordLengthCount.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
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
