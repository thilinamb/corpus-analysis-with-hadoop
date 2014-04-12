package cs455.hadoop.job;

import cs455.hadoop.combine.BookMetricCombiner;
import cs455.hadoop.map.BookMetricMapper;
import cs455.hadoop.reduce.BookMetricReducer;
import cs455.hadoop.type.BookMetricInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * User: thilinab
 * Date: 4/12/14
 * Time: 2:40 PM
 */
public class BookMetricCalculator {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            // make sure that each file is considered as a separate split.
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status_list = fs.listStatus(new Path(args[0]));
            if (status_list != null) {
                for (FileStatus status : status_list) {
                    FileInputFormat.addInputPath(job, status.getPath());
                }
            }

            job.setJarByClass(BookMetricCalculator.class);
            job.setJobName("Book Metric Calculation");
            // set the output path
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(BookMetricMapper.class);
            job.setReducerClass(BookMetricReducer.class);
            job.setCombinerClass(BookMetricCombiner.class);
            // set output for mappers
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(BookMetricInfo.class);
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
