import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordLengthCount {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
		
		private HashMap<Integer,Integer> inMap;
		private IntWritable length = new IntWritable();
		private IntWritable count = new IntWritable();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			inMap = new HashMap<Integer,Integer>();

		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
					
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			int tokenLength;
			
			// In-Map Reducer
			while (tokenizer.hasMoreTokens()) {
				tokenLength = tokenizer.nextToken().length();
				Integer tokenCountInteger = inMap.get(tokenLength);
				int tokenCount = tokenCountInteger == null ? 0 : tokenCountInteger.intValue();
				//tokenCount++;
				inMap.put(tokenLength, ++tokenCount);
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			// Iterated the in-map integrated HashMap
			for (java.util.Map.Entry<Integer, Integer> entry: inMap.entrySet()) {
				length.set(entry.getKey());
				count.set(entry.getValue());
				context.write(length, count);
			}
		}
	}

	public static class Reduce extends 
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
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
		conf.set("mapred.textoutputformat.separator", " ");

		Job job = new Job(conf, "wordlengthcount");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setJarByClass(WordLengthCount.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}