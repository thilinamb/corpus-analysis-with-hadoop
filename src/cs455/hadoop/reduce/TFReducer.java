package cs455.hadoop.reduce;

import cs455.hadoop.type.TFNGramInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Thilina
 * Date: 4/14/14
 */
public class TFReducer extends Reducer<Text, TFNGramInfo, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<TFNGramInfo> values, Context context) throws IOException, InterruptedException {
        // first find the maximum frequency of any nGram.
        int maxFrequency = 0;
        // we need a cache of nGramInfo values because we have to iterate twice.
        List<TFNGramInfo> cache = new ArrayList<TFNGramInfo>();
        for (TFNGramInfo nGramInfo : values) {
            TFNGramInfo cachedCopy = new TFNGramInfo(
                    new Text(nGramInfo.getnGramString().toString()), new IntWritable(nGramInfo.getnGramCount().get()));
            cache.add(cachedCopy);
            if (maxFrequency < nGramInfo.getnGramCount().get()) {
                maxFrequency = nGramInfo.getnGramCount().get();
            }
        }

        // now calculate the TF and store as the final output. Use the cache in this iteration.
        for (TFNGramInfo nGramInfo : cache) {
            int nGramCount = nGramInfo.getnGramCount().get();
            double tfValue = 0.5 + ((0.5 * nGramCount) / maxFrequency);
            String newKey = key + "#" + nGramInfo.getnGramString();
            String valString = nGramCount + "#" + tfValue;
            // final output will of the form
            // doc_id#ngram_str -> ngram_count#tf_val
            context.write(new Text(newKey), new Text(valString));
        }

    }
}
