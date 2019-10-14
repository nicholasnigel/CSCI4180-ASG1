// sample tokenizer
import java.util.*;

public class Test{

    public static void main(String[] args) throws Exception{
        String test = ">what do you would ,,,,,, happ2en? \n are you crazy csci4180";
        String[] tokens = test.split("[\\p{Punct}0-9\\s]+");
        //System.out.println(tokens[0].equals(""));
        List<String> vo = new ArrayList<String>();
        for(String token: tokens){
            if(!token.equals("")) vo.add(token);
        }
        String[] x = vo.toArray(new String[vo.size()]);
        
        String s = new String();
        String a = "  Hello";
        String b = "  World  ";
        String z = a+b;
        z = z.trim();
        System.out.println(z);

        
        
    }
}