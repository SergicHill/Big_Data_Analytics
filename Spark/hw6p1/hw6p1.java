/**
 * Compares two articles using Spark and Java 
 *  @author Serguey Khovansky
 *  @version: March 2016
 * This is solution of the homework 6, problem 1:
 * Save those paragraphs as .txt files and then import them into two Spark RDD objects, paragraphA and paragraphB. 
 * Use Spark transformation functions to transform those initial RDD-s into RDD-s that contain only words. 
 * List for us the first 10 words in each RDD. Subsequently create RDD-s that contain only unique words in each
 * of paragraphs. Then create an RDD that contains only words that are present in paragraphA but not in paragraphB. 
 * Finally create an RDD that contains only the words common to two paragraphs.
 * Place the code in:
 *1.	Place the code for hw6p1.java  and mySplitter.java into the folder:
 * /home/joe/Documents/Harvard63/HW6/hw6_problem1/src/main/java/hw6problem1
 *
 * 2.	Place the maven  pom.xls into the folder:
 * /home/joe/Documents/Harvard63/HW6/hw6_problem1
 * [joe@localhost hw6_problem1]$ mvn clean && mvn compile && mvn package
 *
 * To run:
 * from /home/joe/Documents/Harvard63/HW6/hw6_problem1
 *... hw6_problem1]$ spark-submit --class hw6problem1.hw6p1 ./target/spark-example-0.0.2.jar file:///home/joe/Documents/Harvard63/HW6/hw6_problem1/hw6p1_data/hw6p1_data_a.txt file:///home/joe/Documents/Harvard63/HW6/hw6_problem1/hw6p1_data/hw6p1_data_b.txt  file:///home/joe/Documents/Harvard63/HW6/hw6_problem1/out100316_k
 *
 */

package hw6problem1; 

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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;


/**
 * The class hw6p1 make database operations with two text files using SPARK MapReduce technology
 */
public class hw6p1 {
  

 /** 
  * @param args[]  input and output paths and location of the file input and output files
 */
  public static void main(String[] args) throws Exception {
    String inputFile1 = args[0];
    String inputFile2 = args[1];
    String outputFile = args[2];
    String outputFile1= outputFile+"_1";
    String outputFile2= outputFile+"_2";
  

  /**
   * Create a Java Spark Context sc
  */ 
    SparkConf conf = new SparkConf().setAppName("hw6p1");
    JavaSparkContext sc = new JavaSparkContext(conf);
		
    /**
    *	Load our input data.
    */
    JavaRDD<String> input1 = sc.textFile(inputFile1);
    JavaRDD<String> input2 = sc.textFile(inputFile2);
   
   
   /**
     * Regex for splitting up into words.
    */
    String sregex = "\\W+";


	/**
	* Class, whic method call that split up a text into words and place the output into RDD
	*/
    mySplitter  MySplitter = new mySplitter(sregex ); 
    JavaRDD<String> paragraphA =  MySplitter.call(input1);
    JavaRDD<String> paragraphB =  MySplitter.call(input2);

	/**
	*  Get 10 lines of each RDD
	*/
    List<String>  list_A_10 =  paragraphA.take(10);
    List<String>  list_B_10 =  paragraphB.take(10);

	/**
	* Convert each List into RDD
	*/
    JavaRDD<String> rddlist_A = sc.parallelize(list_A_10);
    JavaRDD<String> rddlist_B = sc.parallelize(list_B_10);


	/**
	* RDD that contains only unique words in each of paragraphs.
	*/
    JavaRDD<String> rddUniqueA_B =paragraphB.subtract(paragraphA).union(paragraphA.subtract(paragraphB) ).distinct();
   
    /**
	*  RDD that contains only words that are present in paragraphA but not in paragraphB.
	*/
    JavaRDD<String> rddUniqueAonly =paragraphA.distinct().subtract(paragraphB).distinct();
	
	
	/**
	* RDD that contains only the words common to two paragraphs
	*/
    JavaRDD<String> rddIntersection_AB =paragraphA.intersection(paragraphB);

	/**
	* Output paths
	*/
    String outputFile_union = outputFile+"u";
    String outputFile_onlyA = outputFile+"Aonly"; 
    String outputFile_ABintersec = outputFile+"ABintersec";

	/**
	* save the RDD into folders given by the output strings above
	*/
    rddlist_A.saveAsTextFile(outputFile1);
    rddlist_B.saveAsTextFile(outputFile2);
    rddUniqueA_B.saveAsTextFile(outputFile_union);
    rddUniqueAonly.saveAsTextFile(outputFile_onlyA);
    rddIntersection_AB.saveAsTextFile(outputFile_ABintersec );

    sc.stop();
	}
}
