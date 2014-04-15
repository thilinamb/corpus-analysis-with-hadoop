package cs455.hadoop.reduce;

import cs455.hadoop.type.TFNGramInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/14/14
 */
public class TFReducer extends Reducer<Text, TFNGramInfo, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<TFNGramInfo> values, Context context) throws IOException, InterruptedException {
        // first find the maximum frequency of any nGram.
        int maxFrequency = 0;
        for (TFNGramInfo nGramInfo : values) {
            if (maxFrequency < nGramInfo.getnGramCount().get()) {
                maxFrequency = nGramInfo.getnGramCount().get();
            }
        }
        // now calculate the TF and store as the final output.
        for (TFNGramInfo nGramInfo : values) {
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
