package shiyan;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class reduce extends Reducer<Text, Text,Text, Text> {
    Text v=new Text();
   private ConcurrentHashMap<String,String> map= new ConcurrentHashMap<String,String>();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value:values) {
            // id time stasus
            String[] fileds = value.toString().split("\t");
            String id = fileds[0];
            String ps = fileds[1];
            String hasstatus = map.get(id);
            //如果没有装载过，直接装入
            if (hasstatus==null){
                map.put(id,ps);
            }else {
            //装载过的id再次判断，value相同返回0
                hasstatus="0".equals(hasstatus)?ps:"1";
                map.put(id,hasstatus);
            }
        }
        int carnumber=map.size();
        int sum = map.reduce(1,(x,y)->Integer.parseInt(y),(x,y)->x+y);
        v.set(String.format("%d\t%.2f",carnumber,sum/(carnumber*1.0)));
        context.write(key,v);
        /*Set<Map.Entry<String, String>> entries = map.entrySet();
        for (Iterator<Map.Entry<String, String>> iterator = entries.iterator();iterator.hasNext();){
            Map.Entry<String,String> next=iterator.next();
            String value = next.getValue();
            if ("1".equals(value)){
                sum++;
            }
        }*/
    }
}
