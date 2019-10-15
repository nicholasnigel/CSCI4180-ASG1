import java.util.*;
import java.io.*;

public class Reader {

    protected String getInitial(String x) {
        return Character.toString(x.charAt(0));
    }

    protected static String getNeighbors(String[] tokens, int start, int N){
        String result = new String();   
        start = start+1;
        int end = tokens.length-1; // last index
        int i=0; // number of neighbors
        while(i!=N-1){
            // if empty and last element return null                    
            if(tokens[start].isEmpty()){
                if(start == end) return null; // if empty and the end, return null
                start++;
                continue;
            }
            result = result + tokens[start] + " ";
            start++;
            i++;
        }                                                                       // end of while loop
        return result.trim();

    }  
    public static void main(String[] args) throws Exception {
        String test = "'Well ho$2w are you doing\n";
        String test2 = "im doing fine";

        List<String> ls = new ArrayList<String>();
        ls.add("wow");
        ls.add("are");
        ls.add("you");
        System.out.println(ls.get(0));
        ls.remove(0);
        System.out.println(ls.get(0));
   }                                       // end of main

}                                           // end of reader