package test3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordBean implements WritableComparable<WordBean> {
    private String name;
    private long values;

    public WordBean() {
    }

    public WordBean(String name, long values) {
        this.name = name;
        this.values = values;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getValues() {
        return values;
    }

    public void setValues(long values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "WordBean{" +
                "name='" + name + '\'' +
                ", values=" + values +
                '}';
    }

    @Override
    public int compareTo(WordBean bean) {
        int result;

        if (values>bean.getValues()){
            result = -1;
        }else if (values<bean.getValues()){
            result = 1;
        }else {
            values=bean.getValues();
            result = -1;
        }
        return result;

    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(name);
        output.writeLong(values);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        name = input.readUTF();
        values =input.readLong();

    }


    public void set(String name, long values) {
        this.name=name;
        this.values=values;
    }
}
