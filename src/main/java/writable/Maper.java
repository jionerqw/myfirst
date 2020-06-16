package writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Maper extends Mapper<LongWritable,Text,Text,FlowBean> {
//1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        Text k=new Text();
        FlowBean v=new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        //读取一行数据
        String line = value.toString();
        //切割字段
        String[] fileds = line.split("\t");
        //封装对象，取出手机号
        String filed = fileds[1];
        //取出上行流量和下载流量
        long upFlow1 = Long.parseLong(fileds[fileds.length - 3]);
        long doumFlow1 = Long.parseLong(fileds[fileds.length - 2]);
        //设置 从k，v
        k.set(filed);
        v.set(upFlow1,doumFlow1);
        context.write(k,v);
    }
}
