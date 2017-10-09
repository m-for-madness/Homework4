package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utilClasses.CustomKey;

import java.io.IOException;

public class MapperForCities extends Mapper<LongWritable, Text, CustomKey, Text> {
    CustomKey customKey;
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] cityStack = value.toString().split("\t");
        if (cityStack.length == 2) {
            customKey = new CustomKey(Integer.parseInt(cityStack[0]), 1);
            context.getCounter("q","---CityMapper").increment(1);
            context.write(customKey, new Text(cityStack[1]));
        }
    }
}
