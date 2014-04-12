package cs455.hadoop.type;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: thilinab
 * Date: 4/12/14
 * Time: 1:31 PM
 */
public class BookMetricInfo implements Writable {

    private IntWritable numWords;
    private IntWritable numSentences;
    private IntWritable numSyllables;

    public BookMetricInfo() {
        this.numWords = new IntWritable();
        this.numSentences = new IntWritable();
        this.numSyllables = new IntWritable();
    }

    public BookMetricInfo(IntWritable numWords, IntWritable numSentences, IntWritable numSyllables) {
        this.numWords = numWords;
        this.numSentences = numSentences;
        this.numSyllables = numSyllables;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        numWords.write(dataOutput);
        numSentences.write(dataOutput);
        numSyllables.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        numWords.readFields(dataInput);
        numSentences.readFields(dataInput);
        numSyllables.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "BookMetricInfo{" +
                "numWords=" + numWords +
                ", numSentences=" + numSentences +
                ", numSyllables=" + numSyllables +
                '}';
    }

    public IntWritable getNumWords() {
        return numWords;
    }

    public void setNumWords(IntWritable numWords) {
        this.numWords = numWords;
    }

    public IntWritable getNumSentences() {
        return numSentences;
    }

    public void setNumSentences(IntWritable numSentences) {
        this.numSentences = numSentences;
    }

    public IntWritable getNumSyllables() {
        return numSyllables;
    }

    public void setNumSyllables(IntWritable numSyllables) {
        this.numSyllables = numSyllables;
    }
}
