package test3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class reduce extends Reducer<WordBean, NullWritable,WordBean, NullWritable> {
    @Override
    protected void reduce(WordBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key,NullWritable.get());
        
    }
}
