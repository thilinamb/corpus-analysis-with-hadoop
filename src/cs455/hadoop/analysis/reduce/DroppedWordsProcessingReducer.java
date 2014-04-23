package cs455.hadoop.analysis.reduce;

import cs455.hadoop.analysis.type.NGramAnalysisInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This reducer extracts out the words that were not available in every decade.
 * So contains the words that may have dropped off from the usage.
 * But it's hard to determine whether they have dropped off permanently
 * after a certain period because we do not have enough information
 * about the decades/centuries (e.g. whether they are continuous or not).
 * So it's a manual process to filter out such words.
 * To make it easier, the output from this reducer will have the period sorted
 * in the ascending order against each word.
 * Author: Thilina
 * Date: 4/22/14
 */
public class DroppedWordsProcessingReducer extends Reducer<Text, NGramAnalysisInfo, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<NGramAnalysisInfo> values, Context context) throws IOException, InterruptedException {

        List<NGramAnalysisInfo> nGramInfoList = new ArrayList<NGramAnalysisInfo>();

        for (NGramAnalysisInfo info : values) {
            // output only the words which were not available during every decades.
            if (info.getIdfValue() > 0) {
                nGramInfoList.add(new NGramAnalysisInfo(info.getPeriod(),
                        info.getTfValue(),
                        info.getIdfValue(),
                        info.getTfIdfValue()));
            }
        }
        // sort based on the period
        Collections.sort(nGramInfoList);
        // write sorted output
        for(NGramAnalysisInfo info : nGramInfoList){
            context.write(key, new IntWritable(info.getPeriod()));
        }
    }
}
