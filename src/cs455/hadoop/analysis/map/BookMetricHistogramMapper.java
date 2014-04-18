package cs455.hadoop.analysis.map;

import cs455.hadoop.util.Constants;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * User: thilinab
 * Date: 4/17/14
 * Time: 2:09 PM
 */
public class BookMetricHistogramMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // read line by line. Get the Flesch Reading Ease score against and the year of the book and pass it to the
        // reducer.
        String line = value.toString();
        // split the file name(key) and the value
        String[] keyValueSplits = line.split("\t");
        String fileName = keyValueSplits[0];
        String metricString = keyValueSplits[1];
        // now extract the published year.
        String publishedYearStr = fileName.split("[0-9]*-Year")[1].split(".txt")[0];
        int publishedYear;
        if(publishedYearStr.toUpperCase().contains("BC")){
            publishedYearStr = publishedYearStr.replaceFirst("BC","");
            // For books published in BC, multiply the number by -1
            publishedYear = Integer.parseInt(publishedYearStr) * -1;
        } else {
            publishedYear = Integer.parseInt(publishedYearStr);
        }
        // get the Flesh Reading Ease score from the value String.
        // it is of the form flesch_score//reading_level
        int metricName = Integer.parseInt(context.getConfiguration().get(Constants.METRIC_NAME));
        // Check the metric index and read the correct value from the value String
        // it is stored as FLESCH_READING#FLESCH_KINCAID
        int metricIndex = 0;
        if (metricName == Constants.FLESCH_KINCAID) {
            metricIndex = 1;
        }
        Double fleshReadingScore = Double.parseDouble(metricString.split("#")[metricIndex]);

        // check if the histogram should be generated per-decade
        String period = context.getConfiguration().get(Constants.PERIOD).toLowerCase();
        if (period.equals(Constants.PER_DECADE)) {
            // calculate the decade the published year belongs into.
            publishedYear = publishedYear - (publishedYear % 10);
        }

        // write the published year(key) and the Flesch score(value) to the reducer
        context.write(new IntWritable(publishedYear), new DoubleWritable(fleshReadingScore));
    }
}
