import java.util.*;
import java.io.*;

public class Reader {

    protected static String[] splitter(String doc){
        
        String[] tokens = doc.split("[\\p{Punct}0-9\\s]+"); // splitting for anything that is not alphabetic characters
        List<String> temp = new ArrayList<String>();

        for (String token: tokens){
            if (!token.equals("")) temp.add(token);
        }
        String result[] = temp.toArray(new String[temp.size()]);
        return result;

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
        for(String i: tokens){
            System.out.println(i);
        }
   }                                       // end of main

}                                           // end of reader