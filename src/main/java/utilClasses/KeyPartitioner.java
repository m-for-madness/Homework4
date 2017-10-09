package utilClasses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<CustomKey, Text> {

	@Override
	public int getPartition(CustomKey key, Text value, int numPartitions) {
		return (key.getUserId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
