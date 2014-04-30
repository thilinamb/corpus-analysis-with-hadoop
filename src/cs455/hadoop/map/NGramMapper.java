package cs455.hadoop.map;

import cs455.hadoop.util.Constants;
import cs455.hadoop.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Author: Thilina
 * Date: 4/13/14
 */
public class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // read the file name. We'll be using it as the key
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        String line = value.toString();

        // tokenize the elements into an array.
        StringTokenizer tokenizer = new StringTokenizer(line);
        int tokenCount = tokenizer.countTokens();
        String[] tokens = new String[tokenCount];
        int i = 0;
        while (tokenizer.hasMoreTokens()) {
            tokens[i] = tokenizer.nextToken();
            i++;
        }

        //calculate the N-Grams
        Configuration conf = context.getConfiguration();
        String param = conf.get(Constants.NGRAM_SIZE);
        int nGramSize = Integer.parseInt(param);

        // Check whether the N-Grams are calculated per decade or per-book
        String granularity = conf.get(Constants.NGRAM_GRANUALITY).toLowerCase();

        for (int j = 0; j <= (tokenCount - nGramSize); j++) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int k = 0; k < nGramSize; k++) {
                stringBuilder.append(tokens[j + k]);
                // use a space to separate the words in the N-Gram
                stringBuilder.append(" ");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            // use '\t' sign to separate the file name/decade and the N-gram
            // For example, the key will be of the form 068-Year458BC.txt   word1 word2..
            String nGramKey;
            // if the key is the
            if (granularity.equals(Constants.PER_DECADE)) {
                nGramKey = Integer.toString(Util.getDecadeFromYear(Util.getPublishedYearFromFileName(fileName)));
            } else if (granularity.equals(Constants.PER_CENTURY)) {
                nGramKey = Integer.toString(Util.getCenturyFromYear(Util.getPublishedYearFromFileName(fileName)));
            } else {
                nGramKey = fileName;
            }
            nGramKey = nGramKey + "\t" + stringBuilder.toString();
            context.write(new Text(nGramKey), new IntWritable(1));
        }
    }
}
