package cs455.hadoop.analysis.type;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This class is acting as the partitioner.
 * It decides which key should go into which reducer.
 * We use the word as the basis to partition the keys.
 * Author: Thilina
 * Date: 4/22/14
 */
public class NaturalKeyPartitioner extends Partitioner<WordDecadeKey, NGramAnalysisInfo> {

    @Override
    public int getPartition(WordDecadeKey wordDecadeKey, NGramAnalysisInfo nGramAnalysisInfo, int numPartitions) {
        int hash = wordDecadeKey.getWord().hashCode();
        int partition = hash % numPartitions;
        return partition;
    }
}
