package cs455.hadoop.analysis.reduce;

import cs455.hadoop.analysis.type.NGramAnalysisInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * This Reducer will be used to identify the words
 * which continued to be in use.
 * The IDF value is analyzed to identify such words.
 * This takes the input from the same mapper as the dropped
 * word processing reducer.
 * User: thilinab
 * Date: 4/23/14
 * Time: 1:03 PM
 */
public class ContinuedWordsProcessingReducer extends Reducer<Text, NGramAnalysisInfo, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<NGramAnalysisInfo> values, Context context) throws IOException, InterruptedException {
        for (NGramAnalysisInfo info : values) {
            // output only the words which were not available during every decades.
            if (info.getIdfValue() == 0) {
                context.write(key, new IntWritable(info.getPeriod()));
            }
        }
    }
}
