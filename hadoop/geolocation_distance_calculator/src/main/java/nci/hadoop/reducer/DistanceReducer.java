package nci.hadoop.reducer;

import nci.hadoop.mapper.IntermediateOutput;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class DistanceReducer extends Reducer<LongWritable, IntermediateOutput, SchoolDistances, NullWritable>{

    private static final int KM_1 = 1000;

    @Override
    protected void reduce(LongWritable key, Iterable<IntermediateOutput> values, Reducer<LongWritable, IntermediateOutput, SchoolDistances, NullWritable>.Context context) throws IOException, InterruptedException {
        int nearestDistance = -1;
        int farthestDistance = -1;
        int numSchoolsWithin1km = 0;
        int avgSchoolDistanceWithin1km;
        int totSchoolDistanceWithin1Km = 0;
        SchoolDistances distances = new SchoolDistances();
        Iterator<IntermediateOutput> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntermediateOutput output = iterator.next();
            int distance = output.getDistance();
            distances.setId(output.getId());
            if(distance < KM_1) {
                numSchoolsWithin1km++;
                totSchoolDistanceWithin1Km += distance;
            }
            if(nearestDistance == -1 || distance < nearestDistance) {
                nearestDistance = distance;
                distances.setNearestRoll(output.getRoll_no());
            }
            if(farthestDistance == -1 || distance > farthestDistance) {
                farthestDistance = distance;
                distances.setFarthestRoll(output.getRoll_no());
            }
        }
        avgSchoolDistanceWithin1km = numSchoolsWithin1km == 0 ? 0 : (totSchoolDistanceWithin1Km / numSchoolsWithin1km);
        distances.setAvgSchoolDistWithin1Km(avgSchoolDistanceWithin1km);
        distances.setFarthestSchoolDistInMetres(farthestDistance);
        distances.setNumSchoolsWithin1Km(numSchoolsWithin1km);
        distances.setNearestSchoolDistInMetres(nearestDistance);
        distances.setNearestRoll(distances.getNearestRoll().replaceAll("\u0000", ""));
        distances.setFarthestRoll(distances.getFarthestRoll().replaceAll("\u0000", ""));
        context.write(distances, NullWritable.get());
    }
}
