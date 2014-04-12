package cs455.hadoop.reduce;

import cs455.hadoop.type.BookMetricInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: thilinab
 * Date: 4/12/14
 * Time: 2:34 PM
 */
public class BookMetricReducer extends Reducer<Text, BookMetricInfo, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<BookMetricInfo> values, Context context) throws IOException, InterruptedException {
        int totalWordCount = 0;
        int totalSentenceCount = 0;
        int totalSyllableCount = 0;

        for(BookMetricInfo bookMetricInfo : values){
            totalWordCount += bookMetricInfo.getNumWords().get();
            totalSentenceCount += bookMetricInfo.getNumSentences().get();
            totalSyllableCount += bookMetricInfo.getNumSyllables().get();
        }

        // calculate the metrics
        double fleschReadingEase = calculateFleschReadingEase(totalWordCount, totalSentenceCount, totalSyllableCount);
        double fleschKincaidGradeLevel = calculateFleschKincaidGradeLevel(totalWordCount, totalSentenceCount, totalSyllableCount);
        context.write(key, new Text(fleschReadingEase + "#" + fleschKincaidGradeLevel));
    }

    private double calculateFleschReadingEase(int totalWords, int totalSentences, int totalSyllables){
        return 206.835 - 1.015*((double)totalWords/(double)totalSentences) -
                84.6*((double)totalSyllables/(double)totalWords);
    }

    private double calculateFleschKincaidGradeLevel(int totalWords, int totalSentences, int totalSyllables){
        return 0.39*((double)totalWords/(double)totalSentences) +
                11.8*((double)totalSyllables/(double)totalWords) - 15.59;
    }
}
