package cs455.hadoop.map;

import cs455.hadoop.type.BookMetricInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * User: thilinab
 * Date: 4/12/14
 * Time: 1:50 PM
 */
public class BookMetricMapper extends Mapper<LongWritable, Text, Text, BookMetricInfo> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // read the file name. We'll be using it as the key
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();

        String line = value.toString();
        StringTokenizer tok = new StringTokenizer(line);
        int wordCount = tok.countTokens();

        context.write(new Text(filename), new BookMetricInfo(new IntWritable(wordCount)));
    }
}
