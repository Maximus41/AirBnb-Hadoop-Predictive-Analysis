package nci.hadoop.driver;

import nci.hadoop.mapper.HaversineMapper;
import nci.hadoop.mapper.IntermediateOutput;
import nci.hadoop.mapper.ListingSchools;
import nci.hadoop.reducer.DistanceReducer;
import nci.hadoop.reducer.SchoolDistances;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaskDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
                new TaskDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf  = new Configuration();
        DBConfiguration.configureDB(conf, "org.postgresql.Driver",
                "jdbc:postgresql://localhost:5432/ireland_airbnb?defaultRowFetchSize=10000&maxResultBuffer=10p", "postgres", "admin");
        Job job = Job.getInstance(conf, "Distance Calculation Using Haversine");
        job.setJarByClass(TaskDriver.class);
        job.setMapperClass(HaversineMapper.class);
        job.setReducerClass(DistanceReducer.class);
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        String[] fields = {"slno", "id", "lat1", "long1", "roll_no", "ethos", "long2", "lat2"};
        String[] outputFields = {"id", "nearest_roll_no", "farthest_roll_no", "nearest_school_dist", "farthest_school_dist", "schools_count", "avg_distance"};
        DBInputFormat.setInput(job, ListingSchools.class, "listing_schools", null, null, fields);
        DBOutputFormat.setOutput(job, "distances", outputFields);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntermediateOutput.class);
        job.setOutputKeyClass(SchoolDistances.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
