package nci.hadoop.mapper.job2;

import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ListingSchools implements DBWritable {

    private long slno, id;
    private double lat1, long1, lat2, long2;
    private String roll_no;

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.slno);
        statement.setLong(2, this.id);
        statement.setDouble(3, this.lat1);
        statement.setDouble(4, this.long1);
        statement.setString(5, this.roll_no);
        statement.setDouble(6, this.lat2);
        statement.setDouble(8, this.long2);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.slno = resultSet.getLong(1);
        this.id = resultSet.getLong(2);
        this.lat1 = resultSet.getDouble(3);
        this.long1 = resultSet.getDouble(4);
        this.roll_no = resultSet.getString(5);
        this.lat2 = resultSet.getDouble(6);
        this.long2 = resultSet.getDouble(7);
    }


    public long getSlno() {
        return slno;
    }

    public void setSlno(long slno) {
        this.slno = slno;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
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

    public String getRoll_no() {
        return roll_no;
    }

    public void setRoll_no(String roll_no) {
        this.roll_no = roll_no;
    }
}
