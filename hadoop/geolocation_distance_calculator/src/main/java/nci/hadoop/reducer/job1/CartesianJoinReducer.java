package nci.hadoop.reducer.job1;

import nci.hadoop.mapper.job1.CartesianProductData;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CartesianJoinReducer extends Reducer<Text, CartesianProductData, CartesianProductData, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<CartesianProductData> values, Reducer<Text, CartesianProductData, CartesianProductData, NullWritable>.Context context) throws IOException, InterruptedException {
        CartesianProductData data = values.iterator().next();
        data.setRollNo(data.getRollNo().trim().replace("\u0000", ""));
        context.write(data, NullWritable.get());
    }
}
