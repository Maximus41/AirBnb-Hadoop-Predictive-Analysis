package nci.hadoop.reducer;

import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SchoolDistances implements DBWritable {

    private long id;
    private String nearestRoll, farthestRoll;
    private int numSchoolsWithin1Km, nearestSchoolDistInMetres, farthestSchoolDistInMetres, avgSchoolDistWithin1Km;


    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.id);
        statement.setString(2, this.nearestRoll);
        statement.setString(3, this.farthestRoll);
        statement.setInt(4, this.nearestSchoolDistInMetres);
        statement.setInt(5, this.farthestSchoolDistInMetres);
        statement.setInt(6, this.numSchoolsWithin1Km);
        statement.setInt(7, this.avgSchoolDistWithin1Km);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getLong(1);
        this.nearestRoll = resultSet.getString(2);
        this.farthestRoll = resultSet.getString(3);
        this.nearestSchoolDistInMetres = resultSet.getInt(4);
        this.farthestSchoolDistInMetres = resultSet.getInt(5);
        this.numSchoolsWithin1Km = resultSet.getInt(6);
        this.avgSchoolDistWithin1Km = resultSet.getInt(7);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getNearestRoll() {
        return nearestRoll;
    }

    public void setNearestRoll(String nearestRoll) {
        this.nearestRoll = nearestRoll;
    }

    public String getFarthestRoll() {
        return farthestRoll;
    }

    public void setFarthestRoll(String farthestRoll) {
        this.farthestRoll = farthestRoll;
    }

    public int getNumSchoolsWithin1Km() {
        return numSchoolsWithin1Km;
    }

    public void setNumSchoolsWithin1Km(int numSchoolsWithin1Km) {
        this.numSchoolsWithin1Km = numSchoolsWithin1Km;
    }

    public int getNearestSchoolDistInMetres() {
        return nearestSchoolDistInMetres;
    }

    public void setNearestSchoolDistInMetres(int nearestSchoolDistInMetres) {
        this.nearestSchoolDistInMetres = nearestSchoolDistInMetres;
    }

    public int getFarthestSchoolDistInMetres() {
        return farthestSchoolDistInMetres;
    }

    public void setFarthestSchoolDistInMetres(int farthestSchoolDistInMetres) {
        this.farthestSchoolDistInMetres = farthestSchoolDistInMetres;
    }

    public int getAvgSchoolDistWithin1Km() {
        return avgSchoolDistWithin1Km;
    }

    public void setAvgSchoolDistWithin1Km(int avgSchoolDistWithin1Km) {
        this.avgSchoolDistWithin1Km = avgSchoolDistWithin1Km;
    }
}
