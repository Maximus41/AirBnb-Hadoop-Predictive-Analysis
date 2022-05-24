package nci.hadoop.mapper.job2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntermediateOutput implements Writable {

    private long id;
    private int distance;
    private String roll_no;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeInt(this.distance);
        out.writeChars(this.roll_no);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.distance = in.readInt();
        this.roll_no = in.readLine();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public String getRoll_no() {
        return roll_no;
    }

    public void setRoll_no(String roll_no) {
        this.roll_no = roll_no;
    }
}
