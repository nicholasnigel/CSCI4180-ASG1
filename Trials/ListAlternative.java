import java.util.*;


public class ListAlternative {
    public String[] list;
    public int length;

    public ListAlternative(String[] in) {
        length = in.length;
        list = new String[in.length];
        list = in.clone();
        
    }

    public String[] getlist() {
        return this.list;
    }

    /**
     * Check content of eahc 
     * @param o
     * @return
     */
    public boolean equals(Object o){
        if(o instanceof ListAlternative) {
            ListAlternative temp = (ListAlternative) o;

            for(int i=0 ;i<length; i++) {
                if(!this.list[i].equals(temp.list[i])) return false;
            } // end for
            return true;

        } // end if
        return false;
        
    }
} // end of class

