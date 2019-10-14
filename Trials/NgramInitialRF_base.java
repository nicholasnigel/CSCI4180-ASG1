// import statemets
import java.io.IOException;
//import java.util.StringTokenizer;
//import java.util.Map;
//import java.util.HashMap;
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

/**
 * @author Nigel Nicholas 
 * @version 1.0
 */
public class NgramInitialRF_base {
    
    /**
     * splitting string with non-alphabetic characters as delimiters
     * 
     * @param doc: the content of the document
     * @return result
     */
    protected static String[] splitter(String doc){
        
        String[] tokens = doc.split("[\\p{Punct}0-9\\s]+"); // splitting for anything that is not alphabetic characters
        List<String> temp = new ArrayList<String>();

        for (String token: tokens){
            if (!token.equals("")) temp.add(token);
        }
        String result[] = temp.toArray(new String[temp.size()]);
        return result;

    }
    
    public static class NewMapWritable extends MapWritable{

        @Override
        public String toString() {
            StringBuilder res = new StringBuilder();
            Set<Writable> keySet = this.keySet();

            for(Object key: keySet){
                res.append("{"+ key.toString() + " = "+ this.get(key) + "}");

            }// end for loop
            return res.toString();

        } // end toString

       
        
    }// end of class mapwritable

    public static class TextArrayWritable extends ArrayWritable{
        public TextArrayWritable(){
            super(Text.class);
        }// end of constructor TextArrayWritable()
        public TextArrayWritable(String[] texts){
            super(Text.class);
            int len = texts.length;
            Text[] list = new Text[len];
            
            for(int i = 0; i<len; i++) {
                list[i] = new Text(texts[i]);
            }// end for loop    
            set(list);
            
        }// end of constructor TextArrayWritable(String[] strings)

        /**
         * toString method
         * @return
         */
        @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();
            Writable[] components = this.get();
            for(Object comp: components) {
                sb.append(comp.toString()+" ");
            } // end of for loop
            return sb.toString();
        } // end of toString() method

    }// end of class TextArrayWritable

    public static class RFMapper extends Mapper<LongWritable, Text, Text, NewMapWritable>{
        
        

        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int N = context.getConfiguration().getInt("neighbors", 2); // default should return 2 at least if it is not set.
            String[] tokens = splitter(value.toString());
            
            // Process of getting words 1 by 1
            for (int i = 0; i<tokens.length+1-N; i++) {
                Text word = new Text(Character.toString(tokens[i].charAt(0))); // get the initial only
                NewMapWritable freqMap = new NewMapWritable();
                freqMap.clear();
                // only look into N-1 words forward
                //ArrayWritable neighbors = new ArrayWritable(Text.class);
                
                //List<Text> neighbor = new ArrayList<Text>();
                
                String[] list = new String[N-1];
                for(int j=i+1, k=0; j<i+N; j++,k++){
                    list[k] = Character.toString(tokens[j].charAt(0));
                    //neighbor.add(new Text(Character.toString(tokens[j].charAt(0))));
                }
                //neighbors.set(neighbor.toArray(new Text[neighbor.size()]));
                TextArrayWritable neighbors = new TextArrayWritable(list);
                // if exists in map, add value by 1, else create 1 and intialize with 1
                if(freqMap.containsKey(neighbors)){
                    IntWritable count = (IntWritable) freqMap.get(neighbors);
                    //freqMap.put(neighbors, count.get()+1);
                    count.set(count.get() + 1);
                }
                else {
                    freqMap.put(neighbors, new IntWritable(1));
                }
                context.write(word, freqMap); // emitting each word and the map
            }
            
        }

    } // end of Mapper 

    public static class RFReducer extends Reducer<Text, NewMapWritable, Text, NewMapWritable> {

        public NewMapWritable finalMap = new NewMapWritable(); // the final map to be emitted as final product

        protected void reduce(Text key, Iterable<NewMapWritable> values, Context context) throws IOException, InterruptedException {
            finalMap.clear();
            for (NewMapWritable value: values) {
                // add the key if the key doesn't exist in finalMap, else add with existing count
                Set<Writable> keysets = value.keySet();
                for(Writable keyval: keysets){
                    IntWritable count = (IntWritable) value.get(keyval);
                    
                    if (finalMap.containsKey(keyval)) {
                        IntWritable temp = (IntWritable) finalMap.get(keyval);
                        temp.set(count.get() + temp.get());
                    }// end if



                    else {
                        finalMap.put(keyval, count);
                    }// end else

                }// end of for loop inner
            } // end of for loop outer
            

            //now the finalMap should be filled
            context.write(key, finalMap);

        } // end of reduce function
    } // end of RFReducer static class

    public static void main(String[] args) throws Exception {
        // Main Class
        Configuration conf = new Configuration();
        Job job = new Job(conf, "NgramInitialRF");
        
        // Input and Output file path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // extra parameters required: N and theta
        conf.setInt("neighbor", new Integer(args[2]));
        conf.setDouble("theta", new Double(args[3]));

        job.setJarByClass(NgramInitialRF.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NewMapWritable.class);

        job.setMapperClass(RFMapper.class);
        job.setReducerClass(RFReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
}