package liuyang;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CarDemo implements Writable, DBWritable {
    private String hours;
    private int cars;
    private double rates;

    public CarDemo(String hours, int cars, double rates) {
        this.hours = hours;
        this.cars = cars;
        this.rates = rates;
    }

    public CarDemo() {
    }

    public String getHours() {
        return hours;
    }

    public void setHours(String hours) {
        this.hours = hours;
    }

    public int getCars() {
        return cars;
    }

    public void setCars(int cars) {
        this.cars = cars;
    }

    public double getRates() {
        return rates;
    }

    public void setRates(double rates) {
        this.rates = rates;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(hours);
        dataOutput.writeInt(cars);
        dataOutput.writeDouble(rates);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       this.hours= dataInput.readUTF();
       this.cars=dataInput.readInt();
       this.rates=dataInput.readDouble();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,hours);
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
