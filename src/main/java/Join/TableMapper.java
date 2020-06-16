package Join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {
    String name;
    Text k=new Text();
    TableBean v=new TableBean();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取切片
       FileSplit split = (FileSplit) context.getInputSplit();
       //获取文件名称
       name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读一行数据
        String line = value.toString();
        if (name.startsWith("order")){
            //切割
            String[] fileds = line.split("\t");
            //封装对象
            v.setOrder_id(fileds[0]);
            v.setP_id(fileds[1]);
            v.setAmount(Integer.parseInt(fileds[2]));
            v.setPname("");
            v.setFlag("order");
            //封装key
            k.set(fileds[1]);}
        else{
            String[] fileds = line.split("\t");
            v.setP_id(fileds[0]);
            v.setPname(fileds[1]);
            v.setFlag("pd");
            v.setAmount(0);
            v.setOrder_id("");

            k.set(fileds[0]);

        }

        context.write(k,v);
    }
}
