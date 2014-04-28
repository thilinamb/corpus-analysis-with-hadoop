package cs455.hadoop.map;

import cs455.hadoop.type.TFIDFNGramInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/15/14
 */
public class TFIDFMapper extends Mapper<LongWritable, Text, Text, TFIDFNGramInfo> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        // Each line of the input is of the following form
        // doc_id   ngram_str   ngram_count tf_val
        String[] valueStrSegments = valueString.split("\t");
        String docId = valueStrSegments[0];
        String nGramString = valueStrSegments[1];
        // split the value and get the TF value.
        double tfVal = Double.parseDouble(valueStrSegments[3]);
        TFIDFNGramInfo TFIDFNGramInfo = new TFIDFNGramInfo(new Text(docId), new DoubleWritable(tfVal));
        context.write(new Text(nGramString), TFIDFNGramInfo);
    }
}
