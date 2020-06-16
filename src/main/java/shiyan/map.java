package shiyan;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class map extends Mapper<LongWritable, Text,Text, Text> {
    Text  k=new Text();
    Text  v=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fileds = value.toString().split("\t");
        if(fileds.length!=7){return;}
        String id = fileds[0];
        String h  = fileds[1].substring(0, fileds[1].lastIndexOf(":"));
        String ps = fileds[fileds.length - 1];
        k.set(h);
        v.set(String.format("%s\t%s",id,ps));
        context.write(k,v);
    }
}
