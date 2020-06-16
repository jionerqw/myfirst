package MapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DistributedCacheDriverMapper extends Mapper
<LongWritable, Text,Text, NullWritable>{
  Map<String,String> pdMap=  new HashMap();
    Text k=new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {


        //获取缓存的文件
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
        //循环读取缓存文件一行
        String line=null;
        while (StringUtils.isNotEmpty(line=reader.readLine())) {
            ;
            //切割
            String[] fileds = line.split("\t");
            //缓存数据到集合
            pdMap.put(fileds[0], fileds[1]);
        }
        //关流
        IOUtils.closeStream(reader);

    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fileds = line.split("\t");
        //获取pid
        String pid=fileds[1];
        String pname=pdMap.get(pid);
        //拼接
        line =  line+"\t"+pname;
        k.set(line);
        context.write(k,NullWritable.get());

    }
}
