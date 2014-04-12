package cs455.hadoop.combine;

import cs455.hadoop.type.BookMetricInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: thilinab
 * Date: 4/12/14
 * Time: 5:21 PM
 */
public class BookMetricCombiner extends Reducer<Text, BookMetricInfo, Text, BookMetricInfo> {
    @Override
    protected void reduce(Text key, Iterable<BookMetricInfo> values, Context context) throws IOException, InterruptedException {
        int totalWordCount = 0;
        int totalSentenceCount = 0;
        int totalSyllableCount = 0;

        for (BookMetricInfo bookMetricInfo : values) {
            totalWordCount += bookMetricInfo.getNumWords().get();
            totalSentenceCount += bookMetricInfo.getNumSentences().get();
            totalSyllableCount += bookMetricInfo.getNumSyllables().get();
        }

        context.write(key, new BookMetricInfo(new IntWritable(totalWordCount),
                new IntWritable(totalSentenceCount),
                new IntWritable(totalSyllableCount)));
    }

}