
import combiner.CombinerForCityData;
import mapper.MapperForCities;
import mapper.MapperForData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reducer.ReducerForCitiesData;
import utilClasses.CustomKey;
import utilClasses.GroupComparator;
import utilClasses.KeyPartitioner;

import java.io.File;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

public class Driver extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            int status = ToolRunner.run(new Driver(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input1> <input2> <output>\n", new String("Main.class"));
            return -1;
        }
        Configuration c = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(Driver.class);
        job.setJobName("Bidding price task");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperForData.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperForCities.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setCombinerClass(CombinerForCityData.class);
        
        job.setReducerClass(ReducerForCitiesData.class);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setCombinerKeyGroupingComparatorClass(GroupComparator.class);

        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
