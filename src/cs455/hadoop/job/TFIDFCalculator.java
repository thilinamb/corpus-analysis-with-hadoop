package cs455.hadoop.job;

import cs455.hadoop.map.TFIDFMapper;
import cs455.hadoop.reduce.TFIDFReducer;
import cs455.hadoop.type.TFIDFNGramInfo;
import cs455.hadoop.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/15/14
 */
public class TFIDFCalculator {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // set the corpus size size
            conf.set(Constants.CORPUS_SIZE, args[2]);

            Job job = Job.getInstance(conf);
            job.setJarByClass(TFCalculator.class);
            job.setJobName("TF-IDF Calculation.");
            // set the input path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // set the output path
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(TFIDFMapper.class);
            job.setReducerClass(TFIDFReducer.class);
            // set output types for mappers
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TFIDFNGramInfo.class);
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
