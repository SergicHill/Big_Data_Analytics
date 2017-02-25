/**
 * Illustrates a BigramCount in Java
 *  @author Serguey Khovansky
 *  @version: February 2016
 * This is solution of the homework 4, problem 4:
 * write a working BigramCount program which would 
 * -count occurrences of every pair of consecutive words. You should clean your words just as you did in the previous problem by removing punctuations and cases. 
 * -However, do not count two words separated by a point at the end of a sentence as a bigram.
 * - If you are an experienced programmer add to the bigram count word pairs in which the first word is the last word on the line and the second word is the first word on the subsequent line. If you are not an experienced programmer, than do not do it. Test your program on a small text file, where for comparison, you could identify bigrams manually. Run your program on Ulysis(4300.txt) file and demonstrate that it works. 
 * -Provide us with the total count of your bigrams, first 20 bigrams and all bigrams containing word the word heaven.
 * - Read your file from the local operating system and write results to the local operating system.
 * To compile use maven:
 * Place the code in:
 * 1.	Place the code for WordCount.java into the folder:
 * /home/joe/Documents/Harvard63/HW4/hw4_problem1/src/main/java/hw4problem4
 *
 * 2.	Place the maven  pom.xls into the folder:
 * /home/joe/Documents/Harvard63/HW4/hw4_problem4
 *
 * The command to compile the code using mavem
 *[joe@localhost hw4_problem4]$ ls
 *hw4_problem4work  pom.xml  src  target
 *[joe@localhost hw4_problem4]$ pwd
 * home/joe/Documents/Harvard63/HW4/hw4_problem4
 * [joe@localhost hw4_problem4]$ mvn clean && mvn compile && mvn package
 *
 *The command to run the code:
 *[joe@localhost hw4_problem4]$ spark-submit --class hw4problem4.hw4_problem4 ./target/spark-example-1.jar file:///mnt/hgfs/sharedfolder/4300.txt file:///mnt/hgfs/sharedfolder/out_22716
 *
 */

 package hw4problem4; 

import java.util.*;
import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;



/**
 * The class hw4_problem4 counts different bigrams in a given text using SPARK MapReduce technology
 */
public class hw4_problem4 {
     
	 /** 
		@param args[]  input and output paths and location of the file input and output files
	 */
    public static void main(String[] args) throws Exception {
    String inputFile = args[0];
    String outputFile = args[1];
	/**
    * Create a Java Spark Context sc
	*/
    SparkConf conf = new SparkConf().setAppName("hw4_problem4");
    JavaSparkContext sc = new JavaSparkContext(conf);
		
    /**
    *	Load our input data.
	*/
    JavaRDD<String> input = sc.textFile(inputFile);
	
	/**
     * Split up into words.
    */ 
    JavaRDD<String > words = input.flatMap
    (
      new FlatMapFunction<String, String>()
     {
        public Iterable<String> call(String x) 
        {
		// Turn all words into the lowercase by x.toLowerCase()
		// Split in all elements in non-word characters but leave periods, use "\\s+|[^\\w\\.{1}]\\s+" regex
               String[] xtmp =x.toLowerCase().split("\\s+|[^\\w\\.{1}]\\s+");

              List s = new ArrayList();
              String ztmp="";
                
        try{
  		 for(int i = 0; i < xtmp.length; i=i+1)
                 {
                    ztmp="";
                    if(xtmp[i]!=null && !xtmp[i].contains(" ") && !xtmp[i].endsWith(".")  && i+1<xtmp.length && xtmp[i+1]!=null && !xtmp[i+1].contains(" ")) 
                     {
                       ztmp=xtmp[i]+"-"+xtmp[i+1];
                     }
                    if(ztmp!="") s.add(ztmp);
                 }
            }  catch(IndexOutOfBoundsException e){ System.err.println("IndexOutOfBoundsException");}
                 
        return s;
         }
      }
  );

	/**
         * Transform into word and count and sort by Key, using method sortByKey()
         *
         */
    JavaPairRDD<String, Integer> counts = words.mapToPair(
      new PairFunction<String, String, Integer>(){
           public Tuple2<String, Integer> call(String x){ 
	          String sout= x; 
                  return new Tuple2(sout, 1);}
	}
	).reduceByKey(new Function2<Integer, Integer, Integer>()
	{
            public Integer call(Integer x, Integer y)
			{
				return x + y;
			}
	}).sortByKey();

      long totalcount=counts.count();
      System.out.println("Total counts of bigrams = " + totalcount);

      for(int i=0; i<20; i++)
      {
          System.out.println(String.format("The %d \tline is\t",i)+ counts.take(20).get(i) );

      }
			
	JavaPairRDD<String,Integer> SpecificLines = counts.filter(
		new Function<Tuple2<String,Integer>,  Boolean>() {
		public Boolean call(Tuple2<String,Integer>  line){return line._1().contains("heaven");}
		}
	);


      List myList = SpecificLines.toArray();
      int myList_size = myList.size();
      for (int i=0; i<myList_size; i++)
      {
        System.out.println(String.format("Bigrams %d with the \'word\' the ", i)+ myList.get(i));
      }

      
	/**
        * Save the word count back out to a text file, causing evaluation.
	 */
    counts.saveAsTextFile(outputFile);
	}
}

