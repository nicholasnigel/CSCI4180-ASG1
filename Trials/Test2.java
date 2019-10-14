import java.util.*;

public class Test2 {


    public static Map put(Object[] key, Object value) {
        /*
        Map[] res = new HashMap[key.length];
        res[0] = new HashMap<>();
        res[1] = new HashMap<>();
        res[0].put(key[0], res[1]);
        res[1].put(key[1], value);
        System.out.println(res[0].get("x"));
        return res[0];*/

        Map[] res = new HashMap[key.length];
        int i = 0 ;
        for(i=0; i<key.length; i++){
            res[i] = new HashMap<>();
        }
        for(i=0; i<key.length-1; i++) {
            res[i].put(key[i], res[i+1]);
        }
        res[i].put(key[i], value);
        return res[0];
    }

    public static void main(String[] args) throws Exception {
       
        /** 
        String  x = new String("YES");
        Map mapping = new HashMap<>();

        mapping.put(x,1);

        System.out.println(mapping.get("YES"));
        */
        /*
        Map x = new HashMap<>();
        Map y = new HashMap<>();
        y.put("y",1);
        x.put("x", y);
        Map z = (Map)x.get("x");
        System.out.println(z.get("y"));*/

        /*
        Map[] arr = new HashMap[2];
        arr[0] = new HashMap<>();
        arr[1] = new HashMap<>();
        arr[0].put("x", arr[1]);
        arr[1].put("y", 1);
        
        Map z = (Map)arr[0].get("x");
        System.out.println(z.get("y"));*/
        String[] bil = {"x","y", "z"};
        Map master = put(bil, 1);
        System.out.println(master.get("x"));
        
    }
}