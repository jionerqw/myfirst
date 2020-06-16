package group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private int order_id;
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return order_id +"\t" +price;

    }

    @Override
    public void write(DataOutput out) throws IOException {
            out.writeInt(order_id);
            out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        order_id = in.readInt();
        price=in.readDouble();
    }

    //二次排序
    @Override
    public int compareTo(OrderBean bean) {
        int result;
        if (order_id>bean.getOrder_id()){
                result=1;
        }else if (order_id<bean.getOrder_id()){
            result=-1;
        }else {
            result=price > bean.getPrice() ? 1 : -1;
        }
        return result;
    }

}
