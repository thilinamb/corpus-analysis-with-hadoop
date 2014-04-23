package cs455.hadoop.analysis.type;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This class is responsible to ordering the composite key
 * WordDecadeKey.
 * Author: Thilina
 * Date: 4/22/14
 */
public class WDKeyComparator extends WritableComparator {

    public WDKeyComparator() {
        super(WordDecadeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WordDecadeKey key1 = (WordDecadeKey)a;
        WordDecadeKey key2 = (WordDecadeKey)b;
        return key1.compareTo(key2);
    }
}
