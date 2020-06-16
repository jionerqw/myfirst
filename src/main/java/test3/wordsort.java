package test3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wordcont.WordCount;

import java.io.IOException;

public class wordsort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 设置jar加载路径
        job.setJarByClass(wordsort.class);
        // 3 设置map和reduce类
        job.setMapperClass(map.class);
        job.setCombinerClass(reduce.class);


        // 4 设置map输出
        job.setMapOutputKeyClass(WordBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 设置最终输出kv类型
        job.setOutputKeyClass(WordBean.class);
        job.setOutputValueClass(NullWritable.class);
        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 7 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
