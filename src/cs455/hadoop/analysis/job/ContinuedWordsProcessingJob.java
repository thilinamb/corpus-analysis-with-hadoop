package cs455.hadoop.analysis.job;

import cs455.hadoop.analysis.map.NGramAnalysisMapper;
import cs455.hadoop.analysis.reduce.ContinuedWordsProcessingReducer;
import cs455.hadoop.analysis.type.NGramAnalysisInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * This job is to identify the words which are being used throughout all the decades.
 * User: thilinab
 * Date: 4/23/14
 * Time: 1:15 PM
 */
public class ContinuedWordsProcessingJob {

    public static void main(String[] args) {
        try {
            Job job = Job.getInstance(new Configuration());
            job.setJarByClass(ContinuedWordsProcessingJob.class);
            job.setJobName("Identifying the words that are continually used.");

            // set the input/output path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(NGramAnalysisMapper.class);
            job.setReducerClass(ContinuedWordsProcessingReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NGramAnalysisInfo.class);

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
