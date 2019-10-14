
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

    public class NgramInitialCount{

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

        protected static String[] splitter2(String doc){
            StringTokenizer tokenizer = new StringTokenizer(doc);
            List<String> content = new ArrayList<String>();
            while(tokenizer.hasMoreTokens()){
                String temp = tokenizer.nextToken();
                String[] a = temp.split("[^A-Za-z]");
                for(String s: a) {
                    if(!s.equals("")) content.add(s);
                }
            }
            Object[] o = content.toArray();
            String[] tokens = Arrays.copyOf(o, o.length, String[].class);
            return tokens;
        }
        /**
         * Return the initial character of a word in a form of string
         * @param s
         * @return
         */
        protected static String getInitial(String s){
            return Character.toString(s.charAt(0));
        }                                                                               // end of getInitial method

        public static class Map extends Mapper<LongWritable, Text, Text, MapWritable>{
            private Text word; // as key
            private MapWritable map; // as value
            private List<String> visited; // list of visited alphabet characters

            protected void setup(Context context) throws IOException, InterruptedException{
                word = new Text(); // initialization for both word and map
                map = new MapWritable();
                visited = new ArrayList<String>();
            }                                                                           // end of setup
            /**
             * getting the next N characters
             * @param tokens
             * @param start
             * @param N
             * @return
             */
            protected String getNeighbors(String[] tokens, int start, int N){
                String result = new String();
                for(int i=start+1; i<start+N; i++){
                    result = result + getInitial(tokens[i]) + " ";
                }                                                                       // end for
                result = result.trim();
                return result;
            }                                                                           // end of getNeighbors method

            protected Object[] removeEmpty(String[] tokens) {
                List<String> r = new ArrayList<String>();
                for(String i: tokens){
                if(!i.equals("")) r.add(i);
                }
                return r.toArray();
            }                                                                           // end of removeEmpty

            /**
             * emit key value pair where key: word, value: map that counts the occurence of 
             * key and other words
             * @param key
             * @param value
             * @param context
             * @throws IOException
             * @throws InterruptedException
             */
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                int N = Integer.parseInt(conf.get("N"));
                String[] tokens = splitter2(value.toString());
                //String[] t = value.toString().split("[^A-Za-z]");
                //Object[] t1 = removeEmpty(t);
                //String[] tokens = Arrays.copyOf(t1, t1.length, String[].class);

                for(int i=0; i<tokens.length+1-N; i++){
                    map.clear();
                    String w = getInitial(tokens[i]);
                    
                    //if(visited.contains(w)) continue;
                    word.set(w);
                    //visited.add(w);
                    /*
                    for(int j=i; j<tokens.length+1-N; j++) {
                        if(getInitial(tokens[j]).equals(w) ) {
                            String neighborstring = getNeighbors(tokens, j, N);
                            Text neighbortext = new Text(neighborstring);
                            if(map.containsKey(neighbortext)){
                                IntWritable count = (IntWritable) map.get(neighbortext);
                                count.set(count.get() + 1);
                            }
                            else map.put(neighbortext, new IntWritable(1));
                            
                        }
                    }
                    context.write(word, map);
                    */
                    
                    String neighborstring = getNeighbors(tokens, i, N);
                    Text neighbortext = new Text(neighborstring);

                    // put in map, if it is not there yet put new, else add by 1
                    if(map.containsKey(neighbortext)) {
                        IntWritable count = (IntWritable) map.get(neighbortext);
                        count.set(count.get() + 1);     // update the value 
                    }                                                                   // end if
                    else{
                        map.put(neighbortext, new IntWritable(1));
                    }                                                                   // end else

                    context.write(word, map);
                }                                                                       // end for loop

            }                                                                           // end of map function
        }                                                                               // end of mapper static class

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
            
            
        }                                                                               // end of Reducer static class

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            conf.set("N", args[2]);
            
            Job job = new Job(conf, "NgramInitialCount");
            
            // Input and Output file path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
                    
            conf.set("mapreduce.textoutputformat.separator", " ");

            job.setJarByClass(NgramInitialCount.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);

            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }                                                                                           // end main
    }