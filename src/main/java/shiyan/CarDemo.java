package shiyan;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CarDemo implements Writable,DBWritable{
    private String hours;
    private int cars;
    private double rates;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(hours);
        out.writeInt(cars);
        out.writeDouble(rates);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.hours= in.readUTF();
        this.cars=in.readInt();
        this.rates=in.readDouble();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,"hours");
        statement.setInt(2,cars);
        statement.setDouble(3,rates);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.hours=resultSet.getString(1);
        this.cars=resultSet.getInt(2);
        this.rates=resultSet.getDouble(3);
    }
}
