package weblog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Text k=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //解析数据
        boolean result=parseLog(line,context);
        if (!result){
            return;
        }
        //写出
        k.set(line);
        context.write(k,NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] fileds = line.split(" ");
        if (fileds.length > 11) {
            context.getCounter("map","ture").increment(1);
            return true;
        }
        else {
            context.getCounter("map","false").increment(1);
            return false;
        }

    }

    }

