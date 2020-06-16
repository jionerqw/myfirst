package MapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DistributedCacheDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //获取job信心
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //运行Job加载主类
        job.setJarByClass(DistributedCacheDriver.class);
        //关联map
        job.setMapperClass(DistributedCacheDriverMapper.class);
        //设置最终输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置输出输入文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //加载缓存数据
        job.addCacheFile(new URI("file:///d:/bbbb/Rejoin/pd.txt"));
        // 7 Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        //提交
        System.exit(job.waitForCompletion(true)?0:1);


    }
}
