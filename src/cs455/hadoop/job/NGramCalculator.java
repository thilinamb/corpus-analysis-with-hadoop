package cs455.hadoop.job;

import cs455.hadoop.map.NGramMapper;
import cs455.hadoop.reduce.NGramReducer;
import cs455.hadoop.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/14/14
 */
public class NGramCalculator {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // set the N-gram size
            conf.set(Constants.NGRAM_SIZE, args[2]);
            // set whether to calculate N-grams per decade or per book
            conf.set(Constants.NGRAM_GRANUALITY, args[3]);

            Job job = Job.getInstance(conf);
            // make sure that each file is considered as a separate split.
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status_list = fs.listStatus(new Path(args[0]));
            if (status_list != null) {
                for (FileStatus status : status_list) {
                    FileInputFormat.addInputPath(job, status.getPath());
                }
            }

            job.setJarByClass(NGramCalculator.class);
            job.setJobName("N-Gram calculation.");
            // set the output path
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(NGramMapper.class);
            job.setReducerClass(NGramReducer.class);
            job.setCombinerClass(NGramReducer.class);

            // set output for mappers and reducers
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

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
