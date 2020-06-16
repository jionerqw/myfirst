package writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
// 1 实现writable接口
//2  反序列化时，需要反射调用空参构造函数，所以必须有
//3  写序列化方法
//4 反序列化方法
//5 反序列化方法读顺序必须和写序列化方法的写顺序必须一致
// 6 编写toString方法，方便后续打印到文本
public class FlowBean implements Writable {
    private  long upFlow;
    private  long doumFlow;
    private  long sumFlow;

    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long doumFlow) {
        this.upFlow = upFlow;
        this.doumFlow = doumFlow;
        sumFlow=upFlow+doumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDoumFlow() {
        return doumFlow;
    }

    public void setDoumFlow(long doumFlow) {
        this.doumFlow = doumFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFlow +"\t"+ doumFlow+"\t"+ sumFlow;

    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(doumFlow);
        out.writeLong(sumFlow);
    }

    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.doumFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    public void set(long upFlow1, long doumFlow1) {
        upFlow = upFlow1;
        doumFlow = doumFlow1;
        sumFlow = upFlow1 + doumFlow1;
    }

}
