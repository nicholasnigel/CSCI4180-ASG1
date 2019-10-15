import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
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
        private Text word; // emitted to be key
        private HashMap<String, MapWritable> secMap; // store each word with 
        private List<String> neighborslist;

        protected void setup(Context context) throws IOException, InterruptedException{
            word  = new Text();
            //map = new MapWritable();
            secMap = new HashMap<String, MapWritable>();
            neighborslist = new ArrayList<String>();
        }

        protected String getInitial(String s){
            return Character.toString(s.charAt(0));
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            int N = Integer.parseInt(conf.get("N"));
            String content = value.toString();
            String[] tokens = content.split("[^A-Za-z]");

            for(int i = 0 ; i < tokens.length; i++) {
                if( tokens[i].isEmpty() ) continue; // skip empty string
                String wordstring = getInitial(tokens[i]);
                neighborslist.add(wordstring);
                if( neighborslist.size() < N ) continue;

                String[] neighborStrings = neighborslist.toArray(new String[0]);
                String k = neighborStrings[0];
                String neighbor = new String();
                for(int j = 1; j < neighborStrings.length; j++){
                    neighbor += neighborStrings[j] + " ";
                }
                neighbor = neighbor.trim();

                MapWritable map;                
                if( secMap.containsKey(k) ) map = secMap.get(k);
                else {
                    map = new MapWritable();
                    secMap.put(k, map);
                }
                
                Text neighbortext = new Text(neighbor);
                if( map.containsKey(neighbortext) ){
                    IntWritable count = (IntWritable) map.get(neighbortext);
                    count.set( count.get() + 1 );
                }
                else{
                    map.put(neighbortext, new IntWritable(1));
                }
                neighborslist.remove(0);
                
            }   
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<String> keysets = secMap.keySet();
            for(String key: keysets) {
                MapWritable v = secMap.get(key);
                Text k = new Text(key);
                context.write(k, v);
            }
        }        
    }

    public static class Reduce extends Reducer<Text, MapWritable, Text, IntWritable> {
        private MapWritable finalmap;
        private Text word;

        protected void setup(Context context) throws IOException, InterruptedException{
            word = new Text();  
            finalmap = new MapWritable();                                            // end of setup method
            
        }     

        protected void merge(MapWritable other){
            Set<Writable> keys = other.keySet();
            for(Writable k: keys) {
                // if finalmap contains key k, add them else create new one
                if(finalmap.containsKey(k)){
                    IntWritable count1 = (IntWritable) finalmap.get(k);
                    IntWritable count2 = (IntWritable) other.get(k);
                    count1.set(count1.get() + count2.get());
                }                                                                   // end of if
                else{
                    finalmap.put((Text) k, (IntWritable) other.get(k));
                }                                                                   // end else

            }                                                                       // end of for loop

        }                                                                           // end of merge method
        
                                                                            
        
        /**
         * aggregate all with the same key to reducer and add up all the MapWritable for final map
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(Text key, Iterable<MapWritable> value, Context context) throws IOException, InterruptedException {
            finalmap.clear();
            for(MapWritable v: value) {
                merge(v);   // merge to finalmap
            }                                                                       // end of for loop
            word.set(key);
            Set<Writable> keys = finalmap.keySet();
            for(Writable k: keys){
                String newstring = new String();
                String rootword = word.toString();
                String added = ((Text) k).toString();
                newstring = newstring+rootword+" "+ added;
                Text newkey = new Text(newstring);
                IntWritable v = (IntWritable) finalmap.get(k);
                context.write(newkey,v);
            }

        }                                                                           // end of reduce method
        
        
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