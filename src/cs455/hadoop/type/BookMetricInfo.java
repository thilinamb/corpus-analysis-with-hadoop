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

    public BookMetricInfo() {
        this.numWords = new IntWritable();
    }

    public BookMetricInfo(IntWritable numWords) {
        this.numWords = numWords;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        numWords.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        numWords.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "BookMetricInfo{" +
                "numWords=" + numWords +
                '}';
    }

    public IntWritable getNumWords() {
        return numWords;
    }

    public void setNumWords(IntWritable numWords) {
        this.numWords = numWords;
    }
}
