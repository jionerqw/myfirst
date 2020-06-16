package test3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class map extends Mapper<LongWritable, Text,WordBean, NullWritable> {
    WordBean bean=new WordBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        String name=split[0];
        long values = Long.parseLong(split[1]);
       bean.set(name,values);
        context.write(bean,NullWritable.get());

    }


}
