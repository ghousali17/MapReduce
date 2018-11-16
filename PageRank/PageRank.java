import java.util.*;
import java.lang.*;
import java.io.*;
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

public class PageRank {
    public static class GenericObject extends GenericWritable  {
        private static Class<? extends Writable>[] CLASSES = null;
        static {
            CLASSES = (Class<? extends Writable>[]) new Class[] {
                PRNodeWritable.class,
                DoubleWritable.class,
            };
        }
    
        //this empty initialize is required by hadoop
        public GenericObject() {
        }
    
        public GenericObject(Writable instance) {
            set(instance);
        }
    
        @Override
        protected Class<? extends Writable>[] getTypes() {
            return CLASSES;
        }
    
        @Override
        public String toString() {
            return get().toString();
        }
    }

    public static class PageRankMapper extends Mapper <LongWritable, Text, LongWritable, GenericObject> {
        private Configuration conf;
        private double numNodes;
        
        @Override
        public void setup (Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.numNodes = Double.parseDouble(conf.get("numNodes"));
        }
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            PRNodeWritable N = new PRNodeWritable();
            DoubleWritable p = new DoubleWritable();

            String line = value.toString();
            String[] sections = line.split(",");
            String[] words;

            int numNeighbours = 0;
            double pageRank = 0.0;

            if (sections.length > 1) {
                words = sections[0].split(" ");
                pageRank = Double.parseDouble(sections[1]);
            } else {
                words = sections[0].split(" ");
                pageRank = 1 / this.numNodes;
            }

            numNeighbours = words.length - 1;

            if (numNeighbours > 0)
                p.set(pageRank / numNeighbours);
            else
                p.set(0.0);

            LongWritable[] adjList = new LongWritable[numNeighbours];
            for (int i = 0; i < numNeighbours; i++) {
                adjList[i] = new LongWritable(Long.parseLong(words[i + 1]));
                context.write(adjList[i], new GenericObject(p));
            }

            N.setNid(Long.parseLong(words[0]));
            N.setAdjList(adjList);
            N.setPageRank(pageRank);
            context.write(new LongWritable(N.getNid()), new GenericObject(N));
        }
    }
    
    public static class PageRankReducer extends Reducer <LongWritable, GenericObject, LongWritable, PRNodeWritable>{
        public void reduce (LongWritable key, Iterable<GenericObject> values, Context context) throws IOException, InterruptedException {
            PRNodeWritable M = null;
            double s = 0.0;

            for (GenericObject val : values) {
                Writable p = val.get();
                if (p instanceof PRNodeWritable) 
                    M = (PRNodeWritable) p;
                else if (p instanceof DoubleWritable) 
                    s = s + ((DoubleWritable)p).get();
            }
            if (M != null) {
                long prevMass = context.getCounter(COUNTER.MASS).getValue();
                double m = prevMass / 1000000000000000.0;
                m += s;
                long newMass = (long)(m * 1000000000000000.0);
                context.getCounter(COUNTER.MASS).setValue(newMass);
               
                M.setPageRank(s);
                context.write(key, M);
            }
        }
    }

    public static enum COUNTER {
        MASS
    };

    public static double runPageRank(String input, String output, Configuration conf) throws Exception {
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        Job job = Job.getInstance(conf, "PageRank");
        
        job.setJarByClass(PageRank.class); 

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(GenericObject.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);
        
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

        if (job.waitForCompletion(true) == false) {
            System.out.println("PageRank failed");
            System.exit(1);
        }

        Counters counters = job.getCounters();
        long mass = (counters.findCounter(COUNTER.MASS).getValue());
        return 1.0 - (mass / 1000000000000000.0);
    }

    public static void main(String[] args) throws Exception {
        String alpha = args[0];
        String theta = args[1];
        String input = args[3];
        String output = args[4];
        int numIter = Integer.parseInt(args[2]);
        int i = 1;
        double mass = 0.0;

        String temp = "/PRtemp/";

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        conf.set("alpha", alpha);
        conf.set("theta", theta);
        
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(output);
        if(fs.exists(path)){
            fs.delete(path, true);
        }
        path = new Path(temp);
        if(fs.exists(path)){
            fs.delete(path, true);
        }

        System.out.println("*********************************************************");
        System.out.println("*                      PREPROCESSING                    *");
        System.out.println("*********************************************************");
        
        long numNodes = PRPreProcess.runPRPreProcess(input, temp + "/input-" + String.valueOf(i), conf);
        conf.set("numNodes", String.valueOf(numNodes));

        String PRinput = new String();
        String PRoutput = new String();
        String APRinput = new String();
        String APRoutput = new String();

        while (i <= numIter) {
            System.out.println("*********************************************************");
            System.out.println("*                      PAGERANK ITER " + i + "                  *");
            System.out.println("*********************************************************");

            PRinput = temp + "/input-" + String.valueOf(i);
            PRoutput = temp + "/tinput-" + String.valueOf(i);
            
            mass = runPageRank(PRinput, PRoutput, conf);

            conf.unset("mass");
            conf.set("mass", String.valueOf(mass));

            System.out.println("*********************************************************");
            System.out.println("*                  PAGERANK ADJUST ITER " + i + "               *");
            System.out.println("*********************************************************");

            APRinput = temp + "/tinput-" + String.valueOf(i);
            if (i == numIter){
                APRoutput = output;
                PRAdjust.runPRAdjustFinal(APRinput, APRoutput, conf);
            }
            else {
                APRoutput = temp + "/input-" + String.valueOf(i+1);
                PRAdjust.runPRAdjust(APRinput, APRoutput, conf);
            }
            i++;
        }
        Path tempPath = new Path(temp);
        if(fs.exists(tempPath)) {
            fs.delete(tempPath, true);
        }
    }
}