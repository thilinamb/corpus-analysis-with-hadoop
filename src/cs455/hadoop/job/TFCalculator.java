package cs455.hadoop.job;

import cs455.hadoop.map.TFMapper;
import cs455.hadoop.reduce.TFReducer;
import cs455.hadoop.type.TFNGramInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/14/14
 */
public class TFCalculator {

    public static void main(String[] args) {
        try {
            Job job = Job.getInstance(new Configuration());
            job.setJarByClass(TFCalculator.class);
            job.setJobName("TF Calculation.");
            // set the input path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // set the output path
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(TFMapper.class);
            job.setReducerClass(TFReducer.class);
            // set output types for mappers
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TFNGramInfo.class);
            // set output for reducers
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}