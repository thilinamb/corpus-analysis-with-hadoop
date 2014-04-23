package cs455.hadoop.analysis.job;

import cs455.hadoop.analysis.map.NGramAnalysisMapper;
import cs455.hadoop.analysis.reduce.DroppedWordsProcessingReducer;
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
 * Author: Thilina
 * Date: 4/22/14
 */
public class DroppedWordsProcessingJob {
    public static void main(String[] args) {
        try {
            Job job = Job.getInstance(new Configuration());
            job.setJarByClass(DroppedWordsProcessingJob.class);
            job.setJobName("Identifying the words that are discontinued in use.");

            // set the input/output path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(NGramAnalysisMapper.class);
            job.setReducerClass(DroppedWordsProcessingReducer.class);

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
