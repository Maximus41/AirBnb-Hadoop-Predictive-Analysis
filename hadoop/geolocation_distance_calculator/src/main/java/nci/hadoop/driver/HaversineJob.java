package nci.hadoop.driver;

import nci.hadoop.mapper.job2.HaversineMapper;
import nci.hadoop.mapper.job2.IntermediateOutput;
import nci.hadoop.mapper.job2.ListingSchools;
import nci.hadoop.reducer.job2.DistanceReducer;
import nci.hadoop.reducer.job2.SchoolDistances;
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

public class HaversineJob extends Configured implements Tool {

    public static void calculateDistance(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
                new HaversineJob(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf  = new Configuration();
        DBConfiguration.configureDB(conf, "org.postgresql.Driver",
                "jdbc:postgresql://localhost:5432/ireland_airbnb?defaultRowFetchSize=10000&maxResultBuffer=10p", args[0], args[1]);
        Job job = Job.getInstance(conf, "Distance Calculation Using Haversine");
        job.setJarByClass(JobDriver.class);
        job.setMapperClass(HaversineMapper.class);
        job.setReducerClass(DistanceReducer.class);
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        String[] fields = {"slno", "id", "lat1", "long1", "roll_no", "lat2", "long2"};
        String[] outputFields = {"id", "nearest_roll_no", "nearest_school_dist", "schools_count"};
        DBInputFormat.setInput(job, ListingSchools.class, "mapped", null, null, fields);
        DBOutputFormat.setOutput(job, "distances", outputFields);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntermediateOutput.class);
        job.setOutputKeyClass(SchoolDistances.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
