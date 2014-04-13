package cs455.hadoop.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/13/14
 */
public class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int nGramCount = 0;
        for (IntWritable num : values) {
            nGramCount += num.get();
        }
        context.write(key, new IntWritable(nGramCount));
    }
}
