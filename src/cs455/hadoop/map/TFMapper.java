package cs455.hadoop.map;

import cs455.hadoop.type.TFNGramInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/14/14
 */
public class TFMapper extends Mapper<Text, IntWritable, Text, TFNGramInfo> {

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        // Split the key. It's of the form 068-Year458BC.txt#word1 word2
        String[] keyStrSplits = key.toString().split("#");
        String fileName = keyStrSplits[0];
        String nGramString = keyStrSplits[1];
        // now create the intermediate output of the form
        // doc_id -> {nGram string, count}
        context.write(new Text(fileName), new TFNGramInfo(new Text(nGramString), value));
    }
}
