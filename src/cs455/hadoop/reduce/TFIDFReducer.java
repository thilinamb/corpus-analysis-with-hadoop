package cs455.hadoop.reduce;

import cs455.hadoop.type.TFIDFNGramInfo;
import cs455.hadoop.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Thilina
 * Date: 4/15/14
 */
public class TFIDFReducer extends Reducer<Text, TFIDFNGramInfo, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<TFIDFNGramInfo> values, Context context) throws IOException, InterruptedException {
        // get the corpus size from the configuration
        Configuration conf = context.getConfiguration();
        String param = conf.get(Constants.CORPUS_SIZE);
        int corpusSize = Integer.parseInt(param);
        // get the number of documents that the NGram is included in.
        int docFreq = 0;
        List<TFIDFNGramInfo> cache = new ArrayList<TFIDFNGramInfo>();
        for (TFIDFNGramInfo TFIDFNGramInfo : values) {
            TFIDFNGramInfo cachedCopy = new TFIDFNGramInfo(new Text(TFIDFNGramInfo.getDocumentId().toString()),
                    new DoubleWritable(TFIDFNGramInfo.getTfValue().get()));
            cache.add(cachedCopy);
            docFreq++;
        }

        // now calculate the IDF value along with TF-IDF value.
        for (TFIDFNGramInfo TFIDFNGramInfo : cache) {
            double idfVal = Math.log10((double)corpusSize / (double)docFreq);
            double tfIdfVal = idfVal * TFIDFNGramInfo.getTfValue().get();
            String newKeyString = TFIDFNGramInfo.getDocumentId().toString() + "\t" + key.toString();
            // value String is of the following form - tf#idf#tf-idf
            String valueString = TFIDFNGramInfo.getTfValue().get() + "\t" + idfVal + "\t" + tfIdfVal;
            context.write(new Text(newKeyString), new Text(valueString));
        }
    }
}
