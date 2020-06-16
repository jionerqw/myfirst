package writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text,FlowBean,Text,FlowBean> {
    FlowBean v = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow=0;
        long sum_doumFlow=0;
        // 1 遍历所用bean，将其中的上行流量，下行流量分别累加
        for (FlowBean flowBean:values) {
            sum_upFlow += flowBean.getUpFlow();
            sum_doumFlow += flowBean.getDoumFlow();
        }
        // 2 封装对象
        v.set(sum_upFlow, sum_doumFlow);
        // 3 写出
        context.write(key,v);
    }
}
