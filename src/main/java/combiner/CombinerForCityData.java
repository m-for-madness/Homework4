package combiner;

import org.apache.hadoop.io.Text;
import utilClasses.CustomKey;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CombinerForCityData extends Reducer<CustomKey, Text, CustomKey, Text> {

    private final int DATA = 2;
    private final int CITY = 1;

    @Override
    protected void reduce(CustomKey key, Iterable<Text> values, Context con) throws IOException, InterruptedException {
        Integer counter = 0;
        Iterator<Text> iterator = values.iterator();
        if (key.getDataSetType() == CITY) {
            con.write(key, new Text(iterator.next()));
        } else if (key.getDataSetType() == DATA) {
            while (iterator.hasNext()) {
                counter += Integer.parseInt(iterator.next().toString());
            }
            con.write(key, new Text(counter.toString()));
        }

    }
}
