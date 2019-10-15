import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BigramInitialCount {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final int MAP_SIZE = 'z' - 'A' + 1;
        private int[][] inMemoryMapper;
        private char prevChar;

        protected void setup(Context context) throws IOException, InterruptedException {
            inMemoryMapper = new int[MAP_SIZE][MAP_SIZE];
            prevChar = '\0';
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("[^A-Za-z]");
            for (int i = 0; i < tokens.length; i++) {
                String word = tokens[i];

                if (word.isEmpty())
                    continue;

                char nextChar = word.charAt(0);
                if (prevChar != '\0') {
                    int idx0 = prevChar - 'A';
                    int idx1 = nextChar - 'A';
                    inMemoryMapper[idx0][idx1] += 1;
                }
                prevChar = nextChar;
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < MAP_SIZE; i++) {
                for (int j = 0; j < MAP_SIZE; j++) {
                    if (inMemoryMapper[i][j] <= 0)
                        continue;

                    char char0 = (char) ('A' + i);
                    char char1 = (char) ('A' + j);
                    Text K = new Text(char0 + " " + char1);
                    IntWritable V = new IntWritable(inMemoryMapper[i][j]);

                    context.write(K, V);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        // Run on a pseudo-distributed node
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "bigram intial count");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setJarByClass(BigramInitialCount.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}