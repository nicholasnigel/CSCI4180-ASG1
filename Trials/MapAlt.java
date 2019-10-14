// Map Alternative

import java.util.*;

public class MapAlt extends HashMap {

    public MapAlt() {
        super();
    }

    @Override
    public boolean containsKey(Object o) {
        // check key set 1 by 1
        Set<Object> keys = this.keySet();
        for(Object key: keys){
            if (key.equals(o)) return true;
        }
        return false;

    } // end of method containsKey
    
    @Override
    public Object get(Object key) {
        Set<Map.Entry> entries = this.entrySet();
        for(Map.Entry entry: entries) {
            Object a = entry.getKey();
            if(a.equals(key)) return entry.getValue();
        } // end of for loop
        return null;

    } // end of method get

    public Map.Entry find(Object key) {
        Set<Map.Entry> entries = this.entrySet();
        for(Map.Entry entry: entries) {
            Object a = entry.getKey();
            if(a.equals(key)) return entry;
        }
        return null;
    }

    @Override
    public Object put(Object key, Object value) {
        // if get is null just use the normal put
        Map.Entry res = this.find(key);
        if(res==null) {
            
            super.put(key, value);
            return res; 
        }
        // else replace the value
        
        /*
        res = value;
        System.out.println(res);
        return value;*/
        Object prev = res.getValue();
        res.setValue(value);
        return prev;


    } // end of method put
    
} // end of class