package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utilClasses.CustomKey;
import utilClasses.DataParser;

import java.io.IOException;

public class MapperForData extends Mapper<LongWritable, Text, CustomKey, Text> {
    CustomKey customKey;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        DataParser dataParser = new DataParser(value.toString());

        if(dataParser.isValidForMapping()){
            customKey=new CustomKey(dataParser.getCityID(),2);
            context.getCounter("q","---Datamapper").increment(1);
            context.write(customKey,new Text("1"));
        }
    }
}
