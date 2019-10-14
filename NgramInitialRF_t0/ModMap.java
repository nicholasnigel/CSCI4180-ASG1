// Modified Map Suitable for Writing
import java.util.*;
import org.apache.hadoop.io.*;

public class ModMap extends MapWritable {
    
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
        Set<Map.Entry> entries = this.entrySet();
        for(Map.Entry entry: entries) {
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
    public Map.Entry find(Object key) {
        Set<Map.entry> entries = this.entrySet();
        for(Map.Entry entry: entries) {
            Object o = entry.getKey();
            if(a.equals(key)) return entry;
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
        Map.Entry res = this.find(key);
        if(res == null) {
            super.put(key, value);
            return res;
        } // end if
        Object prev = res.getValue();
        res.setValue(value);
        return prev;

    } //end of put method

} // end of clas ModMap