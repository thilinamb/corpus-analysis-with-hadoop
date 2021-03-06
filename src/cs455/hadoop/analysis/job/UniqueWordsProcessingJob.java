package cs455.hadoop.analysis.job;

import cs455.hadoop.analysis.map.NGramAnalysisMapper;
import cs455.hadoop.analysis.reduce.UniqueWordsProcessingReducer;
import cs455.hadoop.analysis.type.NGramAnalysisInfo;
import cs455.hadoop.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * User: thilinab
 * Date: 4/23/14
 * Time: 1:21 PM
 */
public class UniqueWordsProcessingJob {
    public static void main(String[] args) {
        try {
            // set the corpus size to be used inside the reducer
            Configuration conf = new Configuration();
            conf.set(Constants.CORPUS_SIZE, args[2]);

            Job job = Job.getInstance(conf);
            job.setJarByClass(UniqueWordsProcessingJob.class);
            job.setJobName("Identifying the unique words for decades.");

            // set the input/output path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(NGramAnalysisMapper.class);
            job.setReducerClass(UniqueWordsProcessingReducer.class);

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
