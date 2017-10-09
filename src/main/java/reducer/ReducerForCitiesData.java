package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utilClasses.CustomKey;

import java.io.IOException;
import java.util.Iterator;

public class ReducerForCitiesData extends Reducer<CustomKey, Text, Text, IntWritable> {

    protected void reduce(CustomKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        int count = 0;
        Text name= new Text("");
        name = new Text(iterator.next());
        while (iterator.hasNext()) {
            count += Integer.parseInt(iterator.next().toString());
        }
        if (count != 0)
        {
            context.getCounter("q","---REDUCER").increment(1);
            context.write(name, new IntWritable(count));
        }
    }
}
