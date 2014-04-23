package cs455.hadoop.analysis.type;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This class is responsible for grouping the keys that goes into reducers.
 * Now we only consider the word and exclude the decade associated with it.
 * Author: Thilina
 * Date: 4/22/14
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

    public NaturalKeyGroupingComparator() {
        super(WordDecadeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WordDecadeKey key1 = (WordDecadeKey)a;
        WordDecadeKey key2 = (WordDecadeKey)b;

        return key1.getWord().compareTo(key2.getWord());
    }
}
