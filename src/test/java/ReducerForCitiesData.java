import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import utilClasses.CustomKey;

import java.util.ArrayList;
import java.util.List;

public class ReducerForCitiesData {

    ReduceDriver<CustomKey, Text, Text, IntWritable> reduceDriver;


    @Before
    public void setUp() {
        reducer.ReducerForCitiesData reducer = new reducer.ReducerForCitiesData();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducerForCitiesData() throws Exception {
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("lwiw"));
        values.add(new Text(new Integer(3).toString()));
        values.add(new Text(new Integer(2).toString()));
        values.add(new Text(new Integer(5).toString()));
        CustomKey key = new CustomKey(350, 2);
        reduceDriver.withInput(key, values);
        reduceDriver.withOutput(new Text("lwiw"), new IntWritable(10));
        reduceDriver.runTest();
    }
}
