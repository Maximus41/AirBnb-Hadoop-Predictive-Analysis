package nci.hadoop.reducer.job2;

import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SchoolDistances implements DBWritable {

    private long id;
    private String nearestRoll;
    private int numSchoolsWithin1Km, nearestSchoolDistInMetres;


    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.id);
        statement.setString(2, this.nearestRoll);
        statement.setInt(3, this.nearestSchoolDistInMetres);
        statement.setInt(4, this.numSchoolsWithin1Km);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getLong(1);
        this.nearestRoll = resultSet.getString(2);
        this.nearestSchoolDistInMetres = resultSet.getInt(3);
        this.numSchoolsWithin1Km = resultSet.getInt(4);
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
}
