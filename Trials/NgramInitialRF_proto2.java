// imports
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
/**
 * @author Nigel Nicholas
 * @version 1.1
 * 
 */
public class NgramInitialRF_proto2 {

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

    } // end of splitter function
    

    /**
     * Static Class that extends MapWritable to change toString() method
     */
    public static class NewMapWritable extends MapWritable{
        
        /**
         * 
         * @return result 
         */
        @Override
        public String toString() {
            StringBuilder res = new StringBuilder();
            Set<Writable> keySet = this.keySet();

            for (Object key: keySet) {
                res.append("[" + key.toString() + " = "+ this.get(key)+ "]");
            } // end of for loop 
            return res.toString();

        } // end of toString

    } // end of static class NewMapWritable

    /**
     * Extension of ArrayWritable that consists of Text
     */
    public static class TextArrayWritable extends ArrayWritable{
        /**
         * Constructor for TextArrayWritable
         */
        public TextArrayWritable(){
            super(Text.class);
        }// end of constructor TextArrayWritable()
        /**
         * Construction of the Object using String array
         * @param texts
         */
        public TextArrayWritable(String[] texts){
            super(Text.class);
            int len = texts.length;
            Text[] list = new Text[len];
            
            for(int i = 0; i<len; i++) {
                list[i] = new Text(texts[i]);
            }// end for loop    
            set(list);
            
        }// end of constructor TextArrayWritable(String[] strings)

    }// end of class TextArrayWritable


    /**
     * Use of Mapper Class
     */
    public static class Mapping extends Mapper<LongWritable, Text, Text, NewMapWritable> {
        public Text word = new Text();
        public NewMapWritable freqMap = new NewMapWritable();

        
        /**
         * 
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int N = context.getConfiguration().getInt("neighbors", 2); // N : total number of sequential words
            String[] tokens = splitter(value.toString()); 

            for(int i=0 ; i<tokens.length+1-N ; i++) { 
                word.set(Character.toString(tokens[i].charAt(0))); // initial only
                freqMap.clear();
                String[] strings = new String[N-1];
                for(int j=i+1, k=0 ; j<i+N ; j++, k++ ) {
                    strings[k] = Character.toString(tokens[j].charAt(0));
                } // end for loop
                TextArrayWritable neighbors = new TextArrayWritable(strings);

                if(freqMap.containsKey(neighbors)) {
                    IntWritable count = (IntWritable) freqMap.get(neighbors);
                    count.set(count.get() +1);
                } // end if
                else {
                    freqMap.put(neighbors, new IntWritable(1));
                } // end else
                context.write(word, freqMap);

            } // end of for loop 
            

        } // end of map function


        
    } // end of Mapping static class

    public static class Reducing extends Reducer<Text, NewMapWritable, Text, NewMapWritable> {
        public NewMapWritable finalMap = new NewMapWritable();

        /**
         * 
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<NewMapWritable> values, Context context) throws IOException, InterruptedException {
            for(NewMapWritable val: values) {
                Set<Writable> keysets = val.keySet();

                for(Writable k: keysets){
                    IntWritable count = (IntWritable) val.get(k);   // count: the particular dictionary's

                    if(finalMap.containsKey(k)) {
                        IntWritable temp = (IntWritable) finalMap.get(k);
                        temp.set(count.get()+ temp.get());
                    }
                    else finalMap.put(k, count);
                } // end of for loop

            }// end of for loop
            
            context.write(key, finalMap);
        } // end of reduce function
        
    } // end of static class Reducing
    
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = new Job(config, "NgramInitialRF");

        //Input and Output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Extra arguments
        config.setInt("neighbor", new Integer(args[2]));
        config.setDouble("theta", new Double(args[3]));

        job.setJarByClass(NgramInitialRF.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NewMapWritable.class);

        job.setMapperClass(Mapping.class);
        job.setReducerClass(Reducing.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    } // end of main
}

