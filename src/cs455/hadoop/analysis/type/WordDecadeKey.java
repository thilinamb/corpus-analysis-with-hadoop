package cs455.hadoop.analysis.type;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is used as the key when performing data analysis task #4.
 * This represents a composite key comprised of a word and a decade.
 * We define an ordering for the key values.
 * First they are sorted based on the word.
 * Ties are broke based on the decade.
 * This is used for secondary sorting in the MR function created
 * to get an iterator sorted by the year.
 * For example, for the word 'foo', the reducer will get an iterator
 * which is sorted as follows.
 * foo100
 * foo1100
 * f001230
 *
 * Author: Thilina
 * Date: 4/22/14
 */
public class WordDecadeKey implements WritableComparable<WordDecadeKey> {

    private String word;
    private int decade;

    public WordDecadeKey() {

    }

    /**
     *
     * @param word  1-NGram
     * @param decade    Decade the book which contains the N-Gram belongs to.
     */
    public WordDecadeKey(String word, int decade) {
        this.word = word;
        this.decade = decade;
    }

    @Override
    public int compareTo(WordDecadeKey o) {
        // first compare by word.
        int comparisonResult = word.compareTo(o.getWord());
        // if they are equal, compare by decade
        if(comparisonResult == 0){
            comparisonResult = Integer.compare(decade, o.getDecade());
        }
        return comparisonResult;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, word);
        dataOutput.writeInt(decade);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word = WritableUtils.readString(dataInput);
        decade = dataInput.readInt();
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getDecade() {
        return decade;
    }

    public void setDecade(int decade) {
        this.decade = decade;
    }
}
