import combiner.CombinerForCityData;
import mapper.MapperForCities;
import mapper.MapperForData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import reducer.ReducerForCitiesData;
import utilClasses.CustomKey;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DriverTest {

    MapDriver<LongWritable, Text, CustomKey, Text> mapCityDriver;
    MapDriver<LongWritable, Text, CustomKey, Text> mapDataDriver;
    ReduceDriver<CustomKey,Text,Text,IntWritable> reduceDriver;
    ReduceDriver<CustomKey,Text,CustomKey,Text> combineDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        MapperForCities cityMapper = new MapperForCities();
        MapperForData dataMapper = new MapperForData();
        CombinerForCityData dataCombiner = new CombinerForCityData();
        ReducerForCitiesData reducer = new ReducerForCitiesData();
        mapCityDriver = MapDriver.newMapDriver(cityMapper);
        mapDataDriver = MapDriver.newMapDriver(dataMapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        combineDriver = ReduceDriver.newReduceDriver(dataCombiner);
    }


    @Test
    public void testCityMapper() throws IOException {
        final String testLine = "15\tshanxi";
        mapCityDriver.withInput(new LongWritable(0), new Text(
                testLine));
        mapCityDriver.withOutput(new CustomKey(15,1),new Text("shanxi"));
        mapCityDriver.runTest();
    }

    @Test
    public void testDataMapper() throws IOException {
        final String testLine = "82aed71bea7358c9a5be868deae30be0\t20130613000101373\t1\tVh5KZAnrPQTPJIC\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.802.30 Safari/535.1 SE 2.X MetaSr 1.0\t60.187.41.*\t94\t100\t1\ttrqRTuToMTNUjM9r5rMi\t8c96434bb6f4a2aa274a76ad60343f9e\tnull\tmm_17967481_3274913_10742094\t950\t90\t0\t1\t0\tcc9b344e950b4f8c2b96537174a343b7\t350\t29\td29e59bf0f7f8243858b8183f14d4412\t3358\t10063\t0\t0";
        mapDataDriver.withInput(new LongWritable(0), new Text(
                testLine));
        mapDataDriver.withOutput(new CustomKey(100,2),new Text("1"));
        mapDataDriver.runTest();
    }

    @Test
    public void testCombineDriver() throws Exception{
        List<Text> values=new ArrayList<Text>();
        values.add(new Text(new Integer(1).toString()));
        values.add(new Text(new Integer(1).toString()));
        values.add(new Text(new Integer(1).toString()));
        CustomKey key = new CustomKey(350,2);
        combineDriver.withInput(key,values);
        combineDriver.withOutput(key,new Text(new Integer(3).toString()));
        combineDriver.runTest();
    }

    @Test
    public void testReduceDriver() throws  Exception{
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("lwiw"));
        values.add(new Text(new Integer(3).toString()));
        values.add(new Text(new Integer(2).toString()));
        values.add(new Text(new Integer(5).toString()));
        CustomKey key = new CustomKey(350,2);
        reduceDriver.withInput(key,values);
        reduceDriver.withOutput(new Text("lwiw"),new IntWritable(10));
        reduceDriver.runTest();
    }
}
