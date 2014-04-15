package cs455.hadoop.type;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/14/14
 */
public class TFNGramInfo implements Writable {

    private Text nGramString;
    private IntWritable nGramCount;

    public TFNGramInfo() {
        nGramString = new Text();
        nGramCount = new IntWritable();
    }

    public TFNGramInfo(Text nGramString, IntWritable nGramCount) {
        this.nGramString = nGramString;
        this.nGramCount = nGramCount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        nGramString.write(dataOutput);
        nGramCount.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        nGramString.readFields(dataInput);
        nGramCount.readFields(dataInput);
    }

    public Text getnGramString() {
        return nGramString;
    }

    public void setnGramString(Text nGramString) {
        this.nGramString = nGramString;
    }

    public IntWritable getnGramCount() {
        return nGramCount;
    }

    public void setnGramCount(IntWritable nGramCount) {
        this.nGramCount = nGramCount;
    }
}
