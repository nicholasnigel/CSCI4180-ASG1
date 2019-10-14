import java.util.*;
import java.io.*;

public class Reader {

    protected static String[] splitter(String doc){
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
    public static void main(String[] args) throws Exception {
        String test = "'Well ho$2w are you doing\n";
        String test2 = "im doing fine";

        /*
        String[] tokens =  (test+test2).split("\\P{Alpha}+");
        for(String i: tokens){
            System.out.println(i);
        }*/
        String[] tokens = splitter(test+test2);
        for(String i: tokens) {
            System.out.println(i);
        }
   }                                       // end of main

}                                           // end of reader