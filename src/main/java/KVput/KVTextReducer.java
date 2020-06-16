package KVput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KVTextReducer extends Reducer<Text, LongWritable,Text, LongWritable> {
    LongWritable v=new LongWritable(1);
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (LongWritable value:values){
            sum +=value.get();

        }
        v.set(sum);
        context.write(key,v);
    }
}
