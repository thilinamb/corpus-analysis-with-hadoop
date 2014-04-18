package cs455.hadoop.analysis.reduce;

import cs455.hadoop.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: thilinab
 * Date: 4/17/14
 * Time: 3:13 PM
 */
public class BookMetricHistogramReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        // Check whether it's per-book, per-year or per-decade
        Configuration conf = context.getConfiguration();
        String period = conf.get(Constants.PERIOD);

        if (period.toLowerCase().equals(Constants.PER_BOOK)) {
            // write each entry in a separate line
            for (DoubleWritable metricScore : values) {
                context.write(key, metricScore);
            }
        } else if (period.toLowerCase().equals(Constants.PER_YEAR) || period.toLowerCase().equals(Constants.PER_DECADE)) {
            double sum = 0;
            int count = 0;
            for (DoubleWritable score : values) {
                sum += score.get();
                count++;
            }
            double averageScore = sum/(double)count;
            context.write(key, new DoubleWritable(averageScore));
        }

    }
}
