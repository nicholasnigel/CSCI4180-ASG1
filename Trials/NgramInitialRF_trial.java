// IMPORTS
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

public class NgramInitialRF_trial {
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

    public static class RFMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

        public void map(LongWritable key, Text value, Context context) throws Exception {
            int N = context.getConfiguration().getInt("neighbors", 2); // default 2
            String[] tokens = splitter(value.toString()); 

            for(int i=0; i<tokens.length+1-N; i++) {
                Text word = new Text(Character.toString(tokens[i].charAt(0))); // initial only
                Map fmap = new HashMap<>();
                fmap.clear();
                String[] neighbors = new String[N-1];

                for(int j=i+1,k=0; j<i+N; j++,k++) {
                    neighbors[k] = Character.toString(tokens[j].charAt(0));
                } // end inner for loop

                if(fmap.containsKey(neighbors)) {
                    int c = fmap.get(neighbors);
                    fmap.put(neighbors, c+1); //
                }
                else {
                    fmap.put(neighbors,1);
                }

                

            } // end of for loop 
        }
    }
} // end of NgramInitialRF 