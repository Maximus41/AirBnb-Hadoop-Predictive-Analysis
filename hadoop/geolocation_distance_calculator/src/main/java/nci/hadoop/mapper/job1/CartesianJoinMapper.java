package nci.hadoop.mapper.job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class CartesianJoinMapper extends Mapper<LongWritable, AirbnbListing, Text, CartesianProductData> {

    private HashMap<String, ArrayList<SchoolData>> locData;
    @Override
    protected void setup(Mapper<LongWritable, AirbnbListing, Text, CartesianProductData>.Context context) throws IOException, InterruptedException {
        try{
            locData = new HashMap<>();
            readFile(getClass().getClassLoader().getResourceAsStream("schools.txt"));
        } catch(Exception ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    private void readFile(InputStream inputStream) {
        try{
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String schoolRecord = null;
            boolean firstline = true;
            while((schoolRecord = bufferedReader.readLine()) != null) {
                if(firstline) {
                    firstline = false;
                    continue;
                }
                String values[] = schoolRecord.split(",");
                SchoolData data = new SchoolData();
                data.setRollNo(values[0]);
                data.setCounty(values[1]);
                data.setLatitude(Double.parseDouble(values[2]));
                data.setLongitude(Double.parseDouble(values[3]));
                ArrayList<SchoolData> schoolDataArrayList = locData.get(data.getCounty());
                if(schoolDataArrayList != null) {
                    locData.get(data.getCounty()).add(data);
                } else {
                    ArrayList<SchoolData> dataList = new ArrayList<>();
                    dataList.add(data);
                    locData.put(data.getCounty(), dataList);
                }
            }
        }
        catch(IOException ex) {
            System.err.println("Exception while reading schools file: " + ex.getMessage());
        }
    }

    @Override
    protected void map(LongWritable key, AirbnbListing value, Mapper<LongWritable, AirbnbListing, Text, CartesianProductData>.Context context) throws IOException, InterruptedException {
        ArrayList<SchoolData> data = locData.get(value.getCounty());
        if (data == null || data.isEmpty())
            return;
        int count = 0;
        for (SchoolData scd : data) {
            count++;
            CartesianProductData mappedData =  new CartesianProductData();
            mappedData.setSerialNo((value.getId() * 1000) + count);
            mappedData.setId(value.getId());
            mappedData.setRollNo(scd.getRollNo());
            mappedData.setLat1(value.getLatitude());
            mappedData.setLong1(value.getLongitude());
            mappedData.setLat2(scd.getLatitude());
            mappedData.setLong2(scd.getLongitude());
            context.write(new Text(mappedData.getRollNo() + String.valueOf(mappedData.getId())), mappedData);
        }
    }
}
