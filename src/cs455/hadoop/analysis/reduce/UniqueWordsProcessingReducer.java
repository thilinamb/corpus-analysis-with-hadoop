package cs455.hadoop.analysis.reduce;

import cs455.hadoop.analysis.type.NGramAnalysisInfo;
import cs455.hadoop.util.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: thilinab
 * Date: 4/23/14
 * Time: 1:18 PM
 */
public class UniqueWordsProcessingReducer extends Reducer<Text, NGramAnalysisInfo, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<NGramAnalysisInfo> values, Context context) throws IOException, InterruptedException {
        // read the corpus size
        int corpusSize = Integer.parseInt(context.getConfiguration().get(Constants.CORPUS_SIZE));
        // if a particular N-Gram is unique to a decade, then it's IDF = log(corpus_size/1);
        double uniqueIdf = Math.log10((double)corpusSize);

        for (NGramAnalysisInfo info : values) {
            // output only the words which were not available during every decades.
            if (info.getIdfValue() == uniqueIdf) {
                context.write(key, new IntWritable(info.getPeriod()));
            }
        }
    }
}
