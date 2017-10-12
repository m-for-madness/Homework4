import combiner.CombinerForCityData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import utilClasses.CustomKey;

import java.util.ArrayList;
import java.util.List;

public class CombinerForCityDataTest {
    ReduceDriver<CustomKey,Text,CustomKey,Text> combineDriver;

    @Before
    public void setUp() throws Exception {
        CombinerForCityData dataCombiner = new CombinerForCityData();
        combineDriver = ReduceDriver.newReduceDriver(dataCombiner);
    }

    @Test
    public void testCombinerForCityData() throws Exception{
        List<Text> values=new ArrayList<Text>();
        values.add(new Text(new Integer(1).toString()));
        values.add(new Text(new Integer(1).toString()));
        values.add(new Text(new Integer(1).toString()));
        CustomKey key = new CustomKey(350,2);
        combineDriver.withInput(key,values);
        combineDriver.withOutput(key,new Text(new Integer(3).toString()));
        combineDriver.runTest();
    }
}
