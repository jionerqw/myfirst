package Join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orderBean = new ArrayList<TableBean>();
        TableBean pdBean = new TableBean();

        for (TableBean value:values){
            if ("order".equals(value.getFlag())){
                //将数据存到orderBean里面去
                TableBean temp = new TableBean();
                try {
                    BeanUtils.copyProperties(temp,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBean.add(temp);
            }else{
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
       for (TableBean bean:orderBean){
           bean.setPname(pdBean.getPname());
           context.write(bean,NullWritable.get());
       }
    }
}
