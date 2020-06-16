package NinePut;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NLineMapper extends Mapper <LongWritable,Text,Text,LongWritable>{
    Text k=new Text();
    LongWritable v=new LongWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取一行数据
        String line = value.toString();
        //分割字符
        String[] s = line.split(" ");
        //写出
        for (int i = 0; i <s.length ; i++) {
            k.set(s[i]);
           // System.out.println(s[i]);
        }
        context.write(k,v);
    }
}
