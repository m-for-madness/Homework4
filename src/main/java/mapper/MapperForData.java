package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utilClasses.CustomKey;
import utilClasses.DataParser;

import java.io.IOException;

public class MapperForData extends Mapper<LongWritable, Text, CustomKey, Text> {

    CustomKey customKey ;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        DataParser parser = new DataParser(value.toString());
      // if (parser.isValidForMapping()){
            customKey=new CustomKey(parser.getCityID(),2);
            context.write(customKey,new Text("1"));
        //}
        context.getCounter("q","---DataMapper").increment(1);
    }
}
