package cs455.hadoop.reduce;

import cs455.hadoop.type.BookMetricInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: thilinab
 * Date: 4/12/14
 * Time: 2:34 PM
 */
public class BookMetricReducer extends Reducer<Text, BookMetricInfo, Text, BookMetricInfo> {
    @Override
    protected void reduce(Text key, Iterable<BookMetricInfo> values, Context context) throws IOException, InterruptedException {
        int totalWordCount = 0;
        for(BookMetricInfo bookMetricInfo : values){
            totalWordCount += bookMetricInfo.getNumWords().get();
        }

        context.write(key, new BookMetricInfo(new IntWritable(totalWordCount)));
    }
}
