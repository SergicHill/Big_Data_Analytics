/**
 * @author Serguey Khovansky
 * @version February, 2016
 * 
 * This is solution of the homework 2, problem 1:
 * Problem: Modify the class WordCount.java so that its result excludes given stopwords.
 * To run:
 * /bin/$ hadoop jar /home/bin/wordcountstopwords.jar WordCount ulysses out_folder
 * 
 * */

package hw3_problem3;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/** The class WordCount counts different words in a given text using MapReduce technology
 *  it needs to implement the interface Tool in order to use Eclipse environment
 */
public class hw3_problem3 extends Configured implements Tool {
	



	/**The class TokenizerMapper read the file with the text, split each line into words, 
	 * check that the word is not in the stopword set and place it into the context and increase counter by one */
	public static class TokenizerMapper  extends Mapper<Object, Text, Text, IntWritable>
	{   
		/**The number one*/
		private final static IntWritable one = new IntWritable(1);
		
		/**This variable contains the words to be read from the text*/
		private Text word = new Text();
      
		/** This method does the map of Map/Reduce
		 *@param key   integer offset (can be ignored for this task)
		 *@param value  text of the file
		 *@param context  output to the MapReduce framework before being sent to the reduce function 
		 */ 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			/**Split a read value into tokens*/
			StringTokenizer itr = new StringTokenizer(value.toString());
			String nextword = null;
			
			/**Loop while the String itr has more tokens*/
					/**Skip the first token, as it contains the word, and
					 * take the second coin as it contains the number of times this words pops up 
					 * place it into the context*/
					 	itr.nextToken();
					 	nextword=itr.nextToken();
						
					 	word.set(nextword);
						context.write(word, one);
		}
  }
  
	
	/**This class does reduce part of Map/Reduce methodology  */
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	/**Keep the result in the variable result*/
    private IntWritable result = new IntWritable();

    
	/** This method sums up the values, i.e. ones, as related to one key, i.e. the same word
    @param key      word of the textfile
    @param values   number of times a word pops up
    @param context   output of the MapReduce framework
	 */
    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);

      /**Place into the context the word, i.e. key, and the number of times this word pops up in the text*/ 
      context.write(key, result);
    }
  }

     /**This is main method
		@param args[]  input and output paths and location of the file stopword.txt (optional) 
	 */
 	public static void main(String[] args) throws Exception {
 	
 	/** toolrunner is needed to run the code from eclipse */
 	int res = ToolRunner.run(new Configuration(), (Tool) new hw3_problem3(), 	args);
 		
 	System.exit(res);
 	}  
  
 	
 	/**Set different configuation isses
 	 * @param args[]  input and output paths and location of the file stopword.txt (optional) 
 	 * @return 1 (i.e. success) or 0(i.e. failure)
 	 * */	
public int run(String[] args) throws Exception {
	Configuration conf = new Configuration();

	/**Read the arguments given in the command line:
	 * hadoop compiledfile.jar wordcount infolder outfolder  [stopwordfilename]
	 * if the stopwordfile name is not provided it is by default is stopword.txt*/
	
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length < 2)
	{
	   System.err.println("Usage: wordcount <in> [<in>...] <out> [stopWordFile]");
	   System.exit(2);
	} 	
	
	/**Sets the path issues*/	
	Path in = new Path(otherArgs[1]);
	Path out = new Path(otherArgs[2]); 	
	
	/**hadoop jar /home/file.jar WordCount_0 out_folder_1 in_folder_2 stopwords_3.txt
	 * In total there are 4 arguments if stopword file name is provided
	 * */
	
		
	/**Job configuration issues*/
	Job job = Job.getInstance(conf, "WordCount");
	job.setJarByClass(hw3_problem3.class);
    
	FileInputFormat.setInputPaths(job, in);
	FileOutputFormat.setOutputPath(job, out);	

	
	/**Sets key and values of the map class and reducer class*/
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    /**Sets key and values of the output*/
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    boolean success = job.waitForCompletion(true);
	if (success)
		return 0;
	else 
		return 1;
  } 
}
