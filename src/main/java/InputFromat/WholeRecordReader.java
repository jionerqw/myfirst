package InputFromat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;

public class WholeRecordReader  extends RecordReader<Text, BytesWritable> {

    FileSplit split;
    Configuration configuration;
    Text k = new Text();
    BytesWritable v = new BytesWritable();
    boolean isProgress = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 初始化
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        //核心业务
        //定义缓冲区
        if (isProgress){
            byte[] buf = new byte[(int) split.getLength()];
            // 1 获取fs对象
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(configuration);
            // 2 获取输入流
            FSDataInputStream fis = fs.open(path);
            // 3 拷贝
            IOUtils.readFully(fis,buf,0,buf.length);
            // 4 封装v
            v.set(buf,0, buf.length);
            // 5 封装k
            k.set(path.toString());
            // 6 关闭资源
            IOUtils.closeStream(fis);

            isProgress = false;

            return true;
        }

        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        //获取当前的key
        return k;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        //获取当前value
        return v;
    }

    public float getProgress() throws IOException, InterruptedException {
        //获取当前的程序进行的速度
        return 0;
    }

    public void close() throws IOException {
        //关闭资源
    }
}
