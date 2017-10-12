import mapper.MapperForCities;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import utilClasses.CustomKey;

import java.io.IOException;

public class MapperForCitiesTest {
    MapDriver<LongWritable, Text, CustomKey, Text> mapCityDriver;

    @Before
    public void setUp() throws Exception {
        MapperForCities cityMapper = new MapperForCities();
        mapCityDriver = MapDriver.newMapDriver(cityMapper);
    }

    @Test
    public void testMapperForCities() throws IOException {
        final String testLine = "15\tshanxi";
        mapCityDriver.withInput(new LongWritable(0), new Text(
                testLine));
        mapCityDriver.withOutput(new CustomKey(15,1),new Text("shanxi"));
        mapCityDriver.runTest();
    }
}
