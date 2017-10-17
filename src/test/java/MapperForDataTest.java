import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import utilClasses.CustomKey;

import java.io.IOException;

public class MapperForDataTest {
    MapDriver<LongWritable, Text, CustomKey, Text> mapDataDriver;

    @Before
    public void setUp() throws Exception {
        mapper.MapperForData dataMapper = new mapper.MapperForData();
        mapDataDriver = MapDriver.newMapDriver(dataMapper);
    }

    @Test
    public void testMapperForData() throws IOException {
        final String testLine = "82aed71bea7358c9a5be868deae30be0\t20130613000101373\t1\tVh5KZAnrPQTPJIC\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.802.30 Safari/535.1 SE 2.X MetaSr 1.0\t60.187.41.*\t94\t100\t1\ttrqRTuToMTNUjM9r5rMi\t8c96434bb6f4a2aa274a76ad60343f9e\tnull\tmm_17967481_3274913_10742094\t950\t90\t0\t1\t0\tcc9b344e950b4f8c2b96537174a343b7\t350\t29\td29e59bf0f7f8243858b8183f14d4412\t3358\t10063\t0\t0";
        mapDataDriver.withInput(new LongWritable(0), new Text(
                testLine));
        mapDataDriver.withOutput(new CustomKey(100,2),new Text("1"));
        mapDataDriver.runTest();
    }
}
