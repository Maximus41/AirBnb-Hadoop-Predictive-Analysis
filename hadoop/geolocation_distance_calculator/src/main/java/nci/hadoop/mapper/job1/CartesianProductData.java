package nci.hadoop.mapper.job1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CartesianProductData implements DBWritable, WritableComparable<CartesianProductData> {
    private long serialNo, id;
    private double lat1, long1, lat2, long2;
    private String rollNo;

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.serialNo);
        statement.setLong(2, this.id);
        statement.setDouble(3, this.lat1);
        statement.setDouble(4, this.long1);
        statement.setString(5, this.rollNo);
        statement.setDouble(6, this.lat2);
        statement.setDouble(7, this.long2);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.serialNo = resultSet.getLong(1);
        this.id = resultSet.getLong(2);
        this.lat1 = resultSet.getDouble(3);
        this.long1 = resultSet.getDouble(4);
        this.rollNo = resultSet.getString(5);
        this.lat2 = resultSet.getDouble(6);
        this.long2 = resultSet.getDouble(7);
    }

    public long getSerialNo() {
        return serialNo;
    }

    public void setSerialNo(long serialNo) {
        this.serialNo = serialNo;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getRollNo() {
        return rollNo;
    }

    public void setRollNo(String rollNo) {
        this.rollNo = rollNo;
    }

    public double getLat1() {
        return lat1;
    }

    public void setLat1(double lat1) {
        this.lat1 = lat1;
    }

    public double getLong1() {
        return long1;
    }

    public void setLong1(double long1) {
        this.long1 = long1;
    }

    public double getLat2() {
        return lat2;
    }

    public void setLat2(double lat2) {
        this.lat2 = lat2;
    }

    public double getLong2() {
        return long2;
    }

    public void setLong2(double long2) {
        this.long2 = long2;
    }

    @Override
    public int compareTo(CartesianProductData o) {
        return o.id < this.id ? 1 : -1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.serialNo);
        out.writeLong(this.id);
        out.writeDouble(this.lat1);
        out.writeDouble(this.long1);
        out.writeDouble(this.lat2);
        out.writeDouble(this.long2);
        out.writeChars(this.rollNo);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.serialNo = in.readLong();
        this.id = in.readLong();
        this.lat1 = in.readDouble();
        this.long1 = in.readDouble();
        this.lat2 = in.readDouble();
        this.long2 = in.readDouble();
        this.rollNo = in.readLine().toString();
    }
}
