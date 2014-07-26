How to compile the source
=========================
- An Ant based build is supported. Run ant inside the impl directory. The resulting jar will be inside the impl/dist directory.
  The classpath is read from the Hadoop installation directory of the CS department machines.


Contents
========

|- impl - contains the source code and the build.xml file.
|- output - contains the output produced against the original corpus
|- misc - contains various scripts/programs used.
|- README



How to run data analysis tasks
===============================

Tasks 1,2,3
-----------
- Calculate the reading metrics against the corpus.

Command: $HADOOP_HOME/bin/hadoop jar dis/mapred.jar cs455.hadoop.job.BookMetricCalculator <input> <output>

Example:  $HADOOP_HOME/bin/hadoop jar dis/mapred.jar cs455.hadoop.job.BookMetricCalculator /corpus /reading-metrics

This will calculate both reading metrics and it'll record it against the file name. 

- Finally gather the statistics.
Command:	$HADOOP_HOME/bin/hadoop jar dist/mapred.jar cs455.hadoop.analysis.job.BookMetricHistogramJob <input> <output> <metric_name> <granuarity>

The input is the output from TF-IDF calculation job.
'metric_name' is represented using either 1 or 2. These represents Flesch Reading Score and Flesch-Kincaid grade level score respectively.
'granularity' is either 'book', 'year' or 'decade'.

E.g.:		$HADOOP_HOME/bin/hadoop jar dist/mapred.jar cs455.hadoop.analysis.job.BookMetricHistogramJob /reading-metrics /hist-fk-year 2 year


Task 4
-------
First Calculate the N-Grams of size 1 by using decade as the granularity.

Command: $HADOOP_HOME/bin/hadoop jar dist/mapred.jar cs455.hadoop.job.NGramCalculator /corpus /ngrams/length-1 1 decade

Then run the TFCalculator

Command: $HADOOP_HOME/bin/hadoop jar dist/mapred.jar cs455.hadoop.job.TFCalculator /ngrams/length-1 /tf/length-1

Run the TF-IDFCalculator

Command: $HADOOP_HOME/bin/hadoop jar dist/mapred.jar cs455.hadoop.job.TFIDFCalculator /tf/length-1 /tf-idf/length-1 49

* To identify the discontinued words

command: $HADOOP_HOME/bin/hadoop jar dist/mapred.jar cs455.hadoop.analysis.job.DroppedWordsProcessingJob /tf-idf/length-1 /task4/discontinued


*To identify the continued words
command: $HADOOP_HOME/bin/hadoop jar dist/mapred.jar cs455.hadoop.analysis.job.ContinuedWordsProcessingJob /tf-idf/length-1 /task4/continued


Task 5
-------
* Compute the N-grams for length 1,2,3 and 4.

Command: $HADOOP_HOME/bin/hadoop jar dist/mapred.jar cs455.hadoop.job.NGramCalculator /corpus /ngrams/length-x x decade
x is the length of the N-Gram.

* Now compute TF values for all the Ngrams considering them as one input. Use the recursive flag to pass every sub directory as input.

Command: $HADOOP_HOME/bin/hadoop jar dist/mapred.jar cs455.hadoop.job.TFCalculator /ngrams /tf/length-N recursive

* Now calculate the TF-IDF values for all the Ngrams. Use the output from the above command as the input.

Command: $HADOOP_HOME/bin/hadoop jar dist/mapred.jar cs455.hadoop.job.TFIDFCalculator /tf/length-N /tf-idf/length-N 49

* Then run the UniqueWordsProcessingJob.

Command: $HADOOP_HOME/bin/hadoop jar dist/mapred.jar cs455.hadoop.analysis.job.UniqueWordsProcessingJob /tf-idf/length-N /task5/unique 49

*** Running this command for all N-grams produces a large output file which is around 2 GB in size. So this same job was run against the N-grams of size
1 to identify unique words for each century. This output is provided as a proof along with the submission.


Other scripts
-------------
- Gnuplot script(plot-histo) to generate histograms is also submitted.

Command: gnuplot -e "in='<input>';out='<output>'" plot-histo

Example: gnuplot -e "in='FK/hist-fk-book';out='FK/hist-fk-book.png'" plot-histo

- A Java class to get the list of centuries/decades is also provided.

After compiling the class:
	$java GetDecadesFromFileNames <input-dir> <output-file> <century/decade>

