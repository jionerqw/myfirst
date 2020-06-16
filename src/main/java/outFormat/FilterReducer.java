package outFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text, NullWritable,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String line = values.toString();
        line =line+"\r";
        for (NullWritable nullWritable:values) {
            key.set(line);
            context.write(key,NullWritable.get());
        }

    }
}
