import java.util.concurrent.ConcurrentHashMap;

public class gnd {
    public static void main(String[] args) throws  Exception{
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String,String>();
        map.put("1","1");
        map.put("2","1");
        map.put("3","0");
        int size = map.size();
        int sum = map.reduce(1,(x,y)->Integer.parseInt(y),(x,y)->x+y);
        System.out.println(sum/(size*1.0));
    }
}
