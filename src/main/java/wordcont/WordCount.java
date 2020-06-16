package wordcont;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.Map;

public class WordCount {
    public static class Maper extends Mapper<LongWritable,Text,Text, IntWritable>{
       Text k=new Text();
       IntWritable v=new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1 获取一行
            String line = value.toString();
            // 2 切割
            String[] word = line.split("");
            // 3 输出
            for(String words:word){
                k.set(words);
            context.write(k,v);
            }
        }
    }
    public  static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{

        IntWritable v=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           //累加求和
            int sum=0;
            for(IntWritable count:values){
                sum += count.get();
            }
            //输出
            v.set(sum);
            context.write(key,v);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 设置jar加载路径
        job.setJarByClass(WordCount.class);
        // 3 设置map和reduce类
       job.setMapperClass(Maper.class);
       job.setCombinerClass(Reduce.class);
      // job.setReducerClass(Reduce.class);


        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 7 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
