import java.util.*;
import java.io.*;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PRAdjust {
    public static class PRAdjustMapper extends Mapper <LongWritable, Text, LongWritable, PRNodeWritable> {
        private Configuration conf;
        private double alpha;
        private double theta;
        private double mass;
        private double numNodes;
        
        @Override
        public void setup (Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.alpha = Double.parseDouble(conf.get("alpha"));
            this.theta = Double.parseDouble(conf.get("theta"));
            this.mass = Double.parseDouble(conf.get("mass"));
            this.numNodes = Double.parseDouble(conf.get("numNodes"));
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            PRNodeWritable N = new PRNodeWritable();

            String line = value.toString();
            String[] sections = line.split(",");
            String[] words = sections[0].split(" ");;
            int numNeighbours = words.length - 1;
            double pageRank =  Double.parseDouble(sections[1]);

            LongWritable[] adjList = new LongWritable[numNeighbours];
            for (int i = 0; i < numNeighbours; i++) {
                adjList[i] = new LongWritable(Long.parseLong(words[i + 1]));
            }

            double newPageRank = this.alpha * (1.0 / this.numNodes) + (1 - this.alpha) * ((this.mass / this.numNodes) + pageRank);

            N.setNid(Long.parseLong(words[0]));
            N.setAdjList(adjList);
            N.setPageRank(newPageRank);

            context.write(new LongWritable(N.getNid()), N);
        }
    }
    
    public static class PRAdjustReducer extends Reducer <LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {
        public void reduce (LongWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            for (PRNodeWritable val : values) {
                context.write(key, val);
            }
        }
    }

    public static class PRAdjustMapperFinal extends Mapper <LongWritable, Text, LongWritable, DoubleWritable> {
        private Configuration conf;
        private double alpha;
        private double theta;
        private double mass;
        private double numNodes;
        
        @Override
        public void setup (Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.alpha = Double.parseDouble(conf.get("alpha"));
            this.theta = Double.parseDouble(conf.get("theta"));
            this.mass = Double.parseDouble(conf.get("mass"));
            this.numNodes = Double.parseDouble(conf.get("numNodes"));
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] sections = line.split(",");
            String[] words = sections[0].split(" ");
            long nid = Long.parseLong(words[0]);
            double pageRank = Double.parseDouble(sections[1]);

            double newPageRank = this.alpha * (1.0 / this.numNodes) + (1 - this.alpha) * ((this.mass / this.numNodes) + pageRank);

            context.write(new LongWritable(nid), new DoubleWritable(newPageRank));
        }
    }
    
    public static class PRAdjustReducerFinal extends Reducer <LongWritable, DoubleWritable, LongWritable, Text>{
        private Configuration conf;
        private double theta;
        private static final Text V = new Text();
        private static final DecimalFormat df = new DecimalFormat("#.######");

        @Override
        public void setup (Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.theta = Double.parseDouble(conf.get("theta"));
        }

        public void reduce (LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            df.setRoundingMode(RoundingMode.HALF_UP);
            for (DoubleWritable val : values) {
                double pageRank = val.get();
                if (pageRank >= this.theta) {
                    V.set(df.format(pageRank));
                    context.write(key, V);
                }
            }
        }
    }

    public static void runPRAdjust(String input, String output, Configuration conf) throws Exception {
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        Job job = Job.getInstance(conf, "PRAdjust");
        
        job.setJarByClass(PRAdjust.class); 

        job.setMapperClass(PRAdjustMapper.class);
        job.setReducerClass(PRAdjustReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PRNodeWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);
        
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

        if (job.waitForCompletion(true) == false) {
            System.out.println("PRAdjust failed");
            System.exit(1);
        }
    }

    public static void runPRAdjustFinal(String input, String output, Configuration conf) throws Exception {
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        Job job = Job.getInstance(conf, "PRAdjustFinal");
        
        job.setJarByClass(PRAdjust.class); 

        job.setMapperClass(PRAdjustMapperFinal.class);
        job.setReducerClass(PRAdjustReducerFinal.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

        if (job.waitForCompletion(true) == false) {
            System.out.println("PRAdjustFinal failed");
            System.exit(1);
        }
    }
}