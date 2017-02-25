/**
 * @author Serguey Khovansky
 * @version February, 2016
 * 
 * This is solution of the homework 3, problem 4:
 * Problem: 
 * Combine operations of two MapReduce programs in Problems 1 and 3 above into a single program
 *  with chained MapReduce jobs
 * 
 * 
 * */

package hw3_problem4;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/** The class hw3_problem2v2 counts different words in a given text using MapReduce technology
 *  and prints out the results in the decreasing order of the word occurrences
 *  it needs to implement the interface Tool in order to use Eclipse environment 
 */
public class hw3_problem4 extends Configured implements Tool {
	
	
	/** Name of the intermediate folder where the first mapreduce keeps its output*/
	private static final String OUTPUT_PATH1  ="Temp1";
	private static final String OUTPUT_PATH2  ="Temp2";
	

	
	private static String elements[] = {"}","{",")","(" ,"--","-",",",".","I","a","about","an","are","as",
			"at","be","by","com","for","from","how","in","is","it","of","on","or","that","the","this","to",
			"was","what","when","where","who","will","with","the","www" };
	
	private static Set<String> setStopWords = new HashSet<String>(Arrays.asList(elements));

	/**The class TokenizerMapper read the file with the text, split each line into words, 
	 * check that the word is not in the stopword set and place it into the context and increase counter by one
	 * It is a standard wordcount program*/
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
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
				while (itr.hasMoreTokens())
				{
					/**Assign the next token to the string nextword, and if it not in the setStopWords 
					 * place it into the context*/
					nextword = itr.nextToken();

					if(setStopWords.contains(nextword) == false)	
					{
						word.set(nextword);
						context.write(word, one);
					}						
				}
		}
  }
  /** Outcome of the mapper: word1 [1,1,1,1,1]
   * 						 word2 [1,1,1,1]...
   * */
	
	/**This class does reduce part of Map/Reduce methodology  */
     public static class IntSumReducer  extends Reducer<Text,IntWritable,Text,IntWritable> 
     {
	
    	 /**Keep the result in the variable result*/
    	 private IntWritable result = new IntWritable();

    
    	 /** This method sums up the values, i.e. ones, as related to one key, i.e. the same word
    	 @param key      word of the textfile
    	 @param values   number of times a word pops up
    	 @param context   output of the MapReduce framework
    	  */
    	 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    	 {
    		 int sum = 0;
    		 for (IntWritable val : values)
    		 {
    			 sum += val.get();
    		 }
    		 result.set(sum);

    		 /**Place into the context the word, i.e. key, and the number of times this word pops up in the text*/     
    		 context.write(key, result);
    	 }
     }
  /**
   * The classes related to the job1 are above
   */
     
   /***************************************** 
   * JOB2 related classes are below
   ******************************************
   */
   /**
    * This class gets the data from the intermediate folder, named Temp, that was filled out by the first mapreducer job
    * and records them as follows: [number , word]. It sets up the numbers by decreasing order, as prescribed by the
    * comparator job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
    */
    public static class OrderWordsMapper  extends Mapper<Object, Text, LongWritable, Text>
 	{   
 		
 		/**This variable contains the words to be read from the text*/
 		private Text word = new Text();
 		private LongWritable LngWrtble = new LongWritable();
       
 		/** This method does the map of Map/Reduce
 		 *@param key   integer offset (can be ignored for this task)
 		 *@param value  text of the file
 		 *@param context  output to the MapReduce framework before being sent to the reduce function 
 		 */ 
 		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
 		{
 			/**Split a read value into two tokens*/ 			
 			StringTokenizer itr = new StringTokenizer(value.toString());
 			
 			word.set(itr.nextToken());		
 			LngWrtble.set(Integer.parseInt(itr.nextToken()));
 					
 			/**Place in the context the number and word*/
 			context.write(LngWrtble , word );
 		}
   }

     /**This class writes the input directly to the output file
      * Input form the mapper:
      * 10 [word1, word2, word3]
      * 9 [word4, word5]
      * ...
      * 1 [word20, word21, word21]
      * 
      * The output of the reducer
      * 10 word1
      * 10 word2
      * 10 word3
      * 9 word4
      * 
      * Note, the types: LongWritable, Text,LongWritable,  Text
      * */
     public static class NumberOccurances  extends Reducer<LongWritable, Text,LongWritable,  Text>
     {
    		
    	 /** This method makes the record to the output file
    	 @param key      number of occurrences, LongWritable
    	 @param values   word, Text
    	 @param context   output of the MapReduce framework
    	  */   
    	 public void reduce(LongWritable key, Text values ,  Context context) throws IOException, InterruptedException
    	 { 		    	

    		 /**Place into the context  key, i.e. the number of times this word pops up in the text, and the word itself, which is value*/    
    		 context.write(key, values);
    	 }
     }
     /**
      **********************
      * The job2 above
      **********************
      */
     
     /**
      **************************************************** 
      * JOB 3 STARTS
      * **************************************************
      * */
     
     /**The class TokenizerMapper read the file with the text, split each line into words, 
 	 * check that the word is not in the stopword set and place it into the context and increase counter by one */
 	public static class TokenizerMapper2  extends Mapper<Object, Text, IntWritable, IntWritable>
 	{   
 		/**The number one*/
 		private final static IntWritable one = new IntWritable(1);
 		private final static IntWritable num = new IntWritable();
 		
 		/**This variable contains the words to be read from the text*/
 		//private Text word = new Text();
       
 		/** This method does the map of Map/Reduce
 		 *@param key   integer offset (can be ignored for this task)
 		 *@param value  text of the file
 		 *@param context  output to the MapReduce framework before being sent to the reduce function 
 		 */ 
 		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
 		{
 			/**Split a read value into tokens*/
 			StringTokenizer itr = new StringTokenizer(value.toString());
 			//String nextword = null;
 			
 			/**Loop while the String itr has more tokens*/
 					/**Skip the first token, as it contains the word, and
 					 * take the second coin as it contains the number of times this words pops up 
 					 * place it into the context*/
 					 	//itr.nextToken();
 					 	
 					 	num.set(Integer.parseInt(itr.nextToken()));
 						
 					 	//word.set(nextword);
 						context.write(num, one);
 		}


   }
   
 	
 	/**This class does reduce part of Map/Reduce methodology  */
   public static class IntSumReducer2 
        extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
 	
 	/**Keep the result in the variable result*/
     private IntWritable result = new IntWritable();
     //private final static IntWritable num = new IntWritable();
     
 	/** This method sums up the values, i.e. ones, as related to one key, i.e. the same word
     @param key      word of the textfile
     @param values   number of times a word pops up
     @param context   output of the MapReduce framework
 	 */
     public void reduce(IntWritable key, Iterable<IntWritable> values, 
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
     
        
     
     /**
     ************
     * Job 3 above
     * **********
     */
     /**
      * This is main method
	  *	@param args[]  input and output paths and location of the file stopword.txt (optional) 
	  *	It is the same as in the standard wordcount
	 */
 	public static void main(String[] args) throws Exception 
 	{
 		/** toolrunner is needed to run the code from eclipse */	
 		int res = ToolRunner.run(new Configuration(), (Tool) new hw3_problem4(), 	args);	
 		System.exit(res);
 	}  
  
 	
 	/**Set different configuation isses
 	 * @param args[]  input and output paths
 	 * @return 1 (i.e. success) or 0(i.e. failure)
 	 * */	
    public int run(String[] args) throws Exception 
    {
    	Configuration conf = new Configuration();
 
    	/**
    	 * Read the arguments given in the command line:
    	 * hadoop compiledfile.jar wordcount infolder outfolder
    	 * */
	
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	if (otherArgs.length < 2)
    	{
    		System.err.println("Usage: wordcount <in> [<in>...] <out> [stopWordFile]");
    		System.exit(2);
    	} 	
	
    	/**Sets the path issues*/	
    	Path in = new Path(otherArgs[1]);
    	Path out = new Path(otherArgs[2]); 	
	
    	/**
    	 *********************** 
    	 * Job 1 starts
    	 *********************** 
    	 */
	
    	/**Job configuration issues*/
    	
    	Job job = Job.getInstance(conf, "Job1");
    	job.setJarByClass(hw3_problem4.class);
	
    	FileInputFormat.addInputPath(job, in);
    	FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH1 ));
	  
    	/**Sets key and values of the map class and reducer class*/
    	job.setMapperClass(TokenizerMapper.class);
    	job.setCombinerClass(IntSumReducer.class);
    	job.setReducerClass(IntSumReducer.class);
    
    	/**Sets key and values of the output*/
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
      
    	job.waitForCompletion(true);
    	/**
    	 *********************** 
    	 * Job 2 starts
    	 *********************** 
    	 */
    	Job job2 = Job.getInstance(conf, "Job2");
    	job2.setJarByClass(hw3_problem4.class);   
	
    	FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH1 ));
    	FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH2 ));
  	
    
    	/**Sets key and values of the map class and reducer class*/
    	job2.setMapperClass(OrderWordsMapper.class);
	    
    	/**Set comparator for the map class*/
    	job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);	
	    
    	/** Set Map output Key, i.e.LongWritable, and Value, i.e. Text */
    	job2.setMapOutputKeyClass(LongWritable.class);
    	job2.setMapOutputValueClass(Text.class);
	    
    	job2.setCombinerClass(NumberOccurances.class);
    	job2.setReducerClass(NumberOccurances.class);
	    
    	/**Sets key and values of the output*/
    	job2.setOutputKeyClass(LongWritable.class);  
    	job2.setOutputValueClass(Text.class);
	 
    	job2.waitForCompletion(true);

     	/**

    	 *********************** 
    	 * Job 3 starts
    	 *********************** 
    	 */
    	
    
    
    	Job job3 = Job.getInstance(conf, "Job3");
    	job3.setJarByClass(hw3_problem4.class);   
	
	
    	
    	FileInputFormat.addInputPath(job3, new Path(OUTPUT_PATH2 ));
    	FileOutputFormat.setOutputPath(job3, out);
    
    	/**Sets key and values of the map class and reducer class*/
    	job3.setMapperClass(TokenizerMapper2.class);
	    
    	
	    
    	/** Set Map output Key, i.e.LongWritable, and Value, i.e. Text */
    	job3.setMapOutputKeyClass(IntWritable.class);
    	job3.setMapOutputValueClass(IntWritable.class);
	    
    	job3.setCombinerClass(IntSumReducer2.class);
    	job3.setReducerClass(IntSumReducer2.class);
	    
    	
    	
    	/**Sets key and values of the output*/
    	job3.setOutputKeyClass(IntWritable.class);  
    	job3.setOutputValueClass(IntWritable.class);
	 
    	
    	
   	
    	boolean success = job3.waitForCompletion(true);
    	if (success)
    		return 0;
    	else 
    		return 1;
    }
}
