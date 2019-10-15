import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NgramInitialCount {

    public static class Map extends Mapper<LongWritable, Text, Text, MapWritable>{
        
        

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
            conf.set("N", args[2]);
            conf.set("mapreduce.textoutputformat.separator", " ");
            Job job = new Job(conf, "NgramInitialCount");
            
            // Input and Output file path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));                                
            
            job.setJarByClass(NgramInitialCount.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);

            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            
            System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}