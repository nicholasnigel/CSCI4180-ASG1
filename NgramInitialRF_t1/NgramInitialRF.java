import java.util.*;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NgramInitialRF {
    
    public static class TextArrays implements Writable{
        public Text[] texts;
        public int length; // usually determined by Neighbor - 1 attribute
        
        @Override
        public void readFields(DataInput in) throws IOException {
           
            length = in.readInt();        
            texts = new Text[length];        
            for(Text t: texts){
                t = new Text();
                t.readFields(in);
            }
    
        }
    
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(length);
            for(Text x: this.texts) {
                x.write(out);
            }
        }
    
    
    
        public TextArrays(){
            // default is 2
            texts = new Text[10];
        } // end of constructor 
    
        /**
         * Constructor Class for the Class
         * @param in
         */
        public TextArrays(Text[] in) {
            length = in.length;
            texts = new Text[length];
            texts = in.clone();
        } // end of constructor class
    
        /**
         * 
         * @return texts
         */
        public Text[] getTexts() {
            return this.texts;
        }
    
        /**
         * Check whether 2 TextArrays object is the same (content-wise)
         * To be used later on for containsKey in map 
         * @param o
         * @return
         */
        public boolean equals(Object o) {
            if(o instanceof TextArrays) {
                TextArrays temp = (TextArrays) o;
                
                for(int i=0; i<length; i++) {
                    if(!this.texts[i].equals(temp.texts[i])) return false;
                }
                return true;
            } // end if
    
            return false;
        } // end equals() method
    
        /**
         * print each word in sequence separated by space
         * @return result
         */
        public String toString() {
            StringBuilder result = new StringBuilder();
            for(Text word: texts) {
                result.append(word.toString()+" ");
            }
            return result.toString();
        } // end of toString() method
    
    
    } // end of TextArrays
    
    public static class ModMap extends MapWritable {
    
        /**
         * Constructor class of Modified Map data structure
         */
        public ModMap() {
            super();
        }
    
        /**
         * check if this map contains the key: <key>
         * @param key
         * @return true/false
         */
        @Override
        public boolean containsKey(Object key) {
            // check 1 by 1 the set and compare with the key
            Set<Writable> keys = this.keySet();
            for(Writable k: keys) {
                if(k.equals(key)) return true;
            } // end for loop
            return false;
        } // end of function containsKey
    
        /**
         * Get the value from a certain key in the map
         * @param key
         * @return
         */
        @Override
        public Writable get(Object key) {
            Set<Entry<Writable, Writable>> entries = this.entrySet();
            if(entries.isEmpty()) return null;
            for(Entry<Writable, Writable> entry: entries) {
                Object o = entry.getKey();
                if(o.equals(key)) return (Writable) entry.getValue();
            } // end for loop
            return null;
        } // end of method get
    
    
        /**
         * Finding the map entry <Key, Value> pair corresponding to the key
         * @param key
         * @return
         */
        public Entry<Writable,Writable> find(Object key) {
            Set<Entry<Writable,Writable>> entries = this.entrySet();
            for(Entry<Writable,Writable> entry: entries) {
                Object o = entry.getKey();
                if(o.equals(key)) return entry;
            } // end of for loop
            return null;
        } // end of find method
    
    
        /**
         * Putting key value pair into the Map data structure
         * @param key
         * @param value
         * @return
         */
        @Override
        public Writable put(Writable key, Writable value) {
            Entry<Writable,Writable> res = this.find(key);
            if(res == null) {
                super.put(key, value);
                return null;
            } // end if
            Object prev = res.getValue();
            res.setValue(value);
            return (Writable) prev;
    
        } //end of put method
    
    } // end of clas ModMap


    public static class EntryWritable implements Writable {
        private TextArrays textarray;
        private IntWritable count;

        public EntryWritable(Map.Entry<Writable,Writable> entry) {
                
            textarray = (TextArrays) entry.getKey();
            count = (IntWritable) entry.getValue();

        } // end of EntryWritable

            
        public String toString() {
            return(textarray.toString() + count.toString());
        } // end of toString

        @Override
        public void write(DataOutput out) throws IOException {
            textarray.write(out);
            count.write(out);
        } // end of write method

        @Override
        public void readFields(DataInput in) throws IOException {
            textarray.readFields(in);
            count.readFields(in);
        } // end of readFields

    } // end of EntryWritable static class

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
    
        } // end of splitter static method

        public static class RFMapper extends Mapper<LongWritable, Text, Text, ModMap>{
            Text word = new Text();
            ModMap freqMap = new ModMap();
    
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                int N = context.getConfiguration().getInt("neighbors", 2); // get the neighbors
                String[] tokens = splitter(value.toString()); // tokenize word by word 
                
    
                for(int i=0; i<tokens.length+1-N; i++){
                    //Text word = new Text(Character.toString(tokens[i].charAt(0)));
                    freqMap.clear();
                    word.set(Character.toString(tokens[i].charAt(0)));
    
                    Text[] neighborWords = new Text[N-1];
                    for(int j=i+1, k=0; j<i+N; j++,k++) {
                        neighborWords[k] = new Text(Character.toString(tokens[j].charAt(0)));
                    } // end for loop
                    TextArrays neighborslist = new TextArrays(neighborWords);
                    if(freqMap.containsKey(neighborslist)) {
                        IntWritable count = (IntWritable) freqMap.get(neighborslist);
                        count.set(count.get() +1); 
                    } // end if
                    else {
                        freqMap.put(neighborslist, new IntWritable(1));
                    } // end else
                    
                    context.write(word, freqMap); // emit the key value pair for reduce method
    
    
                } // end for loop
    
            } // end of map method
    
        } // end of RFMapper static class
        public static class RFReducer extends Reducer<Text, ModMap, Text, EntryWritable> {
            public ModMap finalMap;
            protected void setup(Context context) throws IOException, InterruptedException {
                finalMap = new ModMap(); // final map to contain all values
            }

            protected void reduce(Text key, Iterable<ModMap> values, Context context) throws IOException, InterruptedException{
                finalMap.clear();
                
                for(ModMap value: values) {
                    Set<Writable> keysets = value.keySet();
                    for(Writable keyval: keysets) { // keyval is of type TextArrays
                        IntWritable count = (IntWritable) value.get(keyval);
    
                        if (finalMap.containsKey(keyval)) {
                            IntWritable temp = (IntWritable) finalMap.get(keyval);
                            temp.set(count.get() + temp.get());
                        } // end if
                        else {
                            finalMap.put((TextArrays)keyval, count);
                        } // end else
    
                    } // end for loop
    
                } // end for loop
                Set<Map.Entry<Writable, Writable>> entrysets = finalMap.entrySet();
                for(Map.Entry<Writable, Writable> ent: entrysets) {
                    EntryWritable finalValue = new EntryWritable(ent);
                    context.write(key, finalValue);
                }
                
            } // end of method reduce
           
           

        }  // end of RFReducer

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "NgramInitialRF");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        conf.setInt("neighbor", new Integer(args[2]));
        conf.setDouble("theta", new Double(args[3]));

        job.setJarByClass(NgramInitialRF.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ModMap.class);

        job.setMapperClass(RFMapper.class);
        job.setReducerClass(RFReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}