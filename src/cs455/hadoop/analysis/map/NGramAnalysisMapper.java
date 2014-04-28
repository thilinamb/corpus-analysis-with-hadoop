package cs455.hadoop.analysis.map;

import cs455.hadoop.analysis.type.NGramAnalysisInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: Thilina
 * Date: 4/22/14
 */
public class NGramAnalysisMapper extends Mapper<LongWritable, Text, Text, NGramAnalysisInfo> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // a line of the input is of the following form.
            //1580  Aaron alone.	0.524390243902439   0.47712125471966244 0.25019773113348154

            // First break the key and the value.
            String[] keyValueSegments = value.toString().split("\t");
            // now split the key to extract the N-gram and the decade.
            int publishedYear = Integer.parseInt(keyValueSegments[0]);
            String nGramString = keyValueSegments[1];

            // now we need to split the value String to get the metrics.
            NGramAnalysisInfo nGramAnalysisInfo = new NGramAnalysisInfo(publishedYear, Double.parseDouble(keyValueSegments[2]),
                    Double.parseDouble(keyValueSegments[3]),
                    Double.parseDouble(keyValueSegments[4]));
            context.write(new Text(nGramString), nGramAnalysisInfo);
    }
}
