package writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        String phonnumber = key.toString().substring(0, 3);
        int Partition=4;
        if("136".equals(phonnumber)){
            Partition=0;
        }else if ("137".equals(phonnumber)){
            Partition=1;
        }else if ("138".equals(phonnumber)){
            Partition=2;
        }else if ("139".equals(phonnumber)){
            Partition=3;
        }
        return Partition;
    }
}
