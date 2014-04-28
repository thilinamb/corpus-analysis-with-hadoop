package cs455.hadoop.map;

import cs455.hadoop.type.TFNGramInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/14/14
 */
public class TFMapper extends Mapper<LongWritable, Text, Text, TFNGramInfo> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // split the line to separate the key and value pair.
        // currently it's of the form
        // 001-Year1263.txt word1 word2 4
        String lineString = value.toString();
        String[] keyValSplit = lineString.split("\t");

        String fileName = keyValSplit[0];
        String nGramString = keyValSplit[1];
        // now create the intermediate output of the form
        // doc_id -> {nGram string, count}
        context.write(new Text(fileName), new TFNGramInfo(new Text(nGramString),
                new IntWritable(Integer.parseInt(keyValSplit[2].trim()))));
    }
}
