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
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String filename = fileSplit.getPath().getName();

        String line = value.toString();
        // get the number of words
        StringTokenizer tok = new StringTokenizer(line);
        int wordCount = tok.countTokens();

        // get the number of  sentences. I'm assuming that the number of '.', '!' and '?'
        // is equal to number of sentences, i.e. 'period' signs are used only to separate
        // sentences.

        int sentenceCount = line.length() - line.replaceAll(".", "").length();
        sentenceCount += line.length() - line.replaceAll("!", "").length();
        sentenceCount += line.length() - line.replaceAll("\\?", "").length();

        // get the syllable count using the code snippet provided in the cs455 Wiki.
        int syllableCount = 0;
        while (tok.hasMoreTokens()) {
            syllableCount += countSyllables(tok.nextToken());
        }

        context.write(new Text(filename), new BookMetricInfo(new IntWritable(wordCount),
                new IntWritable(sentenceCount),
                new IntWritable(syllableCount)));
    }

    /**
     * This function was copied and converted to Java from the CS455 Wiki.
     *
     * @param word Word
     * @return syllable count
     */
    private int countSyllables(String word) {
        char[] vowels = {'a', 'e', 'i', 'o', 'u', 'y'};
        String currentWord = word;
        int numVowels = 0;
        boolean lastWasVowel = false;
        for (char wc : currentWord.toCharArray()) {
            boolean foundVowel = false;
            for (char v : vowels) {
                //don't count diphthongs
                if (v == wc && lastWasVowel) {
                    foundVowel = true;
                    lastWasVowel = true;
                    break;
                } else if (v == wc && !lastWasVowel) {
                    numVowels++;
                    foundVowel = true;
                    lastWasVowel = true;
                    break;
                }
            }

            //if full cycle and no vowel found, set lastWasVowel to false;
            if (!foundVowel)
                lastWasVowel = false;
        }
        //remove es, it's _usually? silent
        if (currentWord.length() > 2 &&
                currentWord.endsWith("es"))
            numVowels--;
            // remove silent e
        else if (currentWord.length() > 1 &&
                currentWord.endsWith("e"))
            numVowels--;

        return numVowels;
    }

}
