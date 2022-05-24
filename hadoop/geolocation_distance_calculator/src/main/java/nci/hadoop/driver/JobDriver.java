package nci.hadoop.driver;

import nci.hadoop.mapper.job1.AirbnbListing;
import nci.hadoop.mapper.job1.CartesianJoinMapper;
import nci.hadoop.mapper.job1.CartesianProductData;
import nci.hadoop.reducer.job1.CartesianJoinReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf  = new Configuration();
        DBConfiguration.configureDB(conf, "org.postgresql.Driver",
                "jdbc:postgresql://localhost:5432/ireland_airbnb?defaultRowFetchSize=10000&maxResultBuffer=10p", args[0], args[1]);
        Job job = Job.getInstance(conf, "Cross Join datasets");
        job.setJarByClass(JobDriver.class);
        job.setMapperClass(CartesianJoinMapper.class);
        job.setReducerClass(CartesianJoinReducer.class);
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        String[] outputFields = {"slno", "id", "lat1", "long1", "roll_no", "lat2", "long2"};
        String[] fields = {"id", "latitude", "longitude", "room_type", "price", "minimum_nights", "number_of_reviews"
        , "reviews_per_month", "calculated_host_listings_count", "availability_365", "number_of_reviews_ltm", "county"};
        DBInputFormat.setInput(job, AirbnbListing.class, "listings", null, null, fields);
        DBOutputFormat.setOutput(job, "mapped", outputFields);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CartesianProductData.class);
        job.setOutputKeyClass(CartesianProductData.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(5);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("Usage: %s needs username/password to access PostgreSQL DB for input/output\n", JobDriver.class.getSimpleName());
            return ;
        }
        int exitCode = ToolRunner.run(new Configuration(),
                new JobDriver(), args);
        if (exitCode == 0)
            HaversineJob.calculateDistance(args);
        else
            System.exit(exitCode);
    }
}
