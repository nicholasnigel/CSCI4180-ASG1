import java.util.*;
import org.apache.hadoop.io.*;


/**
 * Class TextWritables, which is the list of Text objects, to be used as Key in MapWritable
 */
public class TextWritables extends WritableComparable{
    public Text[] texts;
    public int length; // usually determined by Neighbor - 1 attribute
    
    /**
     * Constructor Class for the Class
     * @param in
     */
    public TextWritables(Text[] in) {
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
     * Check whether 2 TextWritables object is the same (content-wise)
     * To be used later on for containsKey in map 
     * @param o
     * @return
     */
    public boolean equals(Object o) {
        if(o instanceof TextWritables) {
            TextWritables temp = (TextWritables) o;
            
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


}