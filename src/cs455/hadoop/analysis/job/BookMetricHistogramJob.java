package cs455.hadoop.analysis.job;

import cs455.hadoop.analysis.map.BookMetricHistogramMapper;
import cs455.hadoop.analysis.reduce.BookMetricHistogramReducer;
import cs455.hadoop.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * User: thilinab
 * Date: 4/17/14
 * Time: 5:06 PM
 */
public class BookMetricHistogramJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set(Constants.METRIC_NAME, args[2]);
            conf.set(Constants.PERIOD, args[3]);

            Job job = Job.getInstance(conf);
            job.setJarByClass(BookMetricHistogramJob.class);
            job.setJobName("Plotting Histograms for Book Metrics for " + args[2] + ":" + args[3]);

            // set the input/output path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(BookMetricHistogramMapper.class);
            job.setReducerClass(BookMetricHistogramReducer.class);

            // set output for mappers
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
