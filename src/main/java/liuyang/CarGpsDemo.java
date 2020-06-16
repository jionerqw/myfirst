package liuyang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class CarGpsDemo {
    public static class CarGpsDemoMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            if(datas.length!=7)return;
            String carId=datas[0];//806584008859
            String hours=datas[1].substring(0,datas[1].lastIndexOf(":")); //2010-09-01 00:00
            String passengerStatus=datas[datas.length-1];
            context.write(new Text(hours),new Text(String.format("%s\t%s",carId,passengerStatus)));
        }
    }
    public static class CarGpsDemoReduce extends Reducer<Text,Text,CarDemo, NullWritable> {
        //2010-09-01 00,{"806584008859 0","806584008851 0","806584008823 1"}
        private ConcurrentHashMap<String,String> map= new ConcurrentHashMap<String,String>();
        @Override
        protected void reduce(Text hours, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val:values){
                String datas[]= val.toString().split("\t");
                String carId=datas[0];
                String passengerStatus=datas[1];
                String hasStatus= map.get(carId);//如果有值，要么0要么是1
                if(hasStatus==null){
                    map.put(carId,passengerStatus);
                }else{
                    //如果map变量获取的值是0，就用最新的passengerStatus最新值覆盖掉
                    // 如果map变量获取的值是1，不论日志的后面是0还是1，都用1覆盖掉
                    hasStatus="0".equals(hasStatus)?passengerStatus:"1";//0,1
                    map.put(carId,hasStatus);
                }
            }
            //独立出租车的数量
            int carNumber=map.size();
            //载客的出租车的数量
            int sum=map.reduce(1,(x,y)->Integer.parseInt(y),(x,y)->x+y);
            CarDemo cd= new CarDemo(hours.toString(),carNumber,sum/(carNumber*1.0));
            context.write(cd, NullWritable.get());
        }
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY,"com.mysql.jdbc.Driver");
        conf.set(DBConfiguration.URL_PROPERTY,"jdbc:mysql://localhost:3306/wordcount");
        conf.set(DBConfiguration.USERNAME_PROPERTY,"root");
        conf.set(DBConfiguration.PASSWORD_PROPERTY,"123456");

        Job job = Job.getInstance(conf);
        job.setJarByClass(CarGpsDemo.class);
        job.setMapperClass(CarGpsDemoMapper.class);
        job.setReducerClass(CarGpsDemoReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\bbbb\\shiyan\\car_gps.log"));
        job.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job,"t_gps","hours","cars","rates");


        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }


}
