import java.util.*;

public class Test3 {
    
    public static void main(String[] args) throws Exception {
        String[] z = {"a","b","c"};
        String[] y = {"x","y","z"};
        String[] w = {"a","b","c"};
        String[] g = {"x","y","z"};
        ListAlternative a = new ListAlternative(z);
        ListAlternative b = new ListAlternative(y);
        ListAlternative c = new ListAlternative(w);
        ListAlternative d = new ListAlternative(g);

        Map mapper = new MapAlt();
        mapper.put(a,1);
        mapper.put(b,2);
        mapper.put(c,3);
        /*
        Set<Map.Entry> entries = mapper.entrySet();
        for(Map.Entry entry: entries){
            System.out.println(entry.getKey().toString() +" "+ entry.getValue().toString());
        } // end of for loop
        */
        System.out.println(mapper.get(c));

        String[] lol = new String[2];
        lol[0] = "yes";
        lol[1] = "yahoo";
        System.out.println(lol[0]);
        Map newmap = new MapAlt();
        System.out.println(newmap.entrySet().isEmpty());
    } // end of main
} // end of Test3