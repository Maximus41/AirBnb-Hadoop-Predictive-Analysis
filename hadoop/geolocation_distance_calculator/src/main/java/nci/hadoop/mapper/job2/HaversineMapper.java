package nci.hadoop.mapper.job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HaversineMapper extends Mapper<LongWritable, ListingSchools, LongWritable, IntermediateOutput> {

    @Override
    protected void map(LongWritable key, ListingSchools value, Mapper<LongWritable, ListingSchools, LongWritable, IntermediateOutput>.Context context) throws IOException, InterruptedException {
        IntermediateOutput output =  new IntermediateOutput();
        double distanceInKm = Haversine.distance(value.getLat1(), value.getLong1(), value.getLat1(), value.getLong2());
        output.setDistance((int)(distanceInKm * 1000));
        output.setId(value.getId());
        output.setRoll_no(value.getRoll_no());
        context.write(new LongWritable(value.getId()), output);
    }
}