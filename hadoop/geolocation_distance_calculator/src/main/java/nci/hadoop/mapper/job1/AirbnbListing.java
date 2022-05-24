package nci.hadoop.mapper.job1;

import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AirbnbListing implements DBWritable {
    private long id;
    private double latitude, longitude, reviewsPerMonth;
    private String roomType, county;
    private int price, minNights, reviewsCount, hostListingsCount, availability, reviewsCountLtm;

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.id);
        statement.setDouble(2, this.latitude);
        statement.setDouble(3, this.longitude);
        statement.setString(4, this.roomType);
        statement.setInt(5, this.price);
        statement.setInt(6, this.minNights);
        statement.setInt(7, this.reviewsCount);
        statement.setDouble(8, this.reviewsPerMonth);
        statement.setInt(9, this.hostListingsCount);
        statement.setInt(10, this.availability);
        statement.setInt(11, this.reviewsCountLtm);
        statement.setString(12, this.county);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getLong(1);
        this.latitude = resultSet.getDouble(2);
        this.longitude = resultSet.getDouble(3);
        this.roomType = resultSet.getString(4);
        this.price = resultSet.getInt(5);
        this.minNights = resultSet.getInt(6);
        this.reviewsCount = resultSet.getInt(7);
        this.reviewsPerMonth = resultSet.getDouble(8);
        this.hostListingsCount = resultSet.getInt(9);
        this.availability = resultSet.getInt(10);
        this.reviewsCountLtm = resultSet.getInt(11);
        this.county = resultSet.getString(12);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getReviewsPerMonth() {
        return reviewsPerMonth;
    }

    public void setReviewsPerMonth(double reviewsPerMonth) {
        this.reviewsPerMonth = reviewsPerMonth;
    }

    public String getRoomType() {
        return roomType;
    }

    public void setRoomType(String roomType) {
        this.roomType = roomType;
    }

    public String getCounty() {
        return county;
    }

    public void setCounty(String county) {
        this.county = county;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public int getMinNights() {
        return minNights;
    }

    public void setMinNights(int minNights) {
        this.minNights = minNights;
    }

    public int getReviewsCount() {
        return reviewsCount;
    }

    public void setReviewsCount(int reviewsCount) {
        this.reviewsCount = reviewsCount;
    }

    public int getHostListingsCount() {
        return hostListingsCount;
    }

    public void setHostListingsCount(int hostListingsCount) {
        this.hostListingsCount = hostListingsCount;
    }

    public int getAvailability() {
        return availability;
    }

    public void setAvailability(int availability) {
        this.availability = availability;
    }

    public int getReviewsCountLtm() {
        return reviewsCountLtm;
    }

    public void setReviewsCountLtm(int reviewsCountLtm) {
        this.reviewsCountLtm = reviewsCountLtm;
    }
}
