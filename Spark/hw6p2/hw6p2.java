/**
 *  @author Serguey Khovansky
 *  @version: March 2016
 * This is solution of the homework 6, problem 2:
 *
 *
 *  Consider attached file emps.txt. It contains: name, age and salary of three employees.
 *  Create RDD emps by importing that file into Spark. Next create a new RDD emps_fields by
 *  transforming the content of every line in RDD emps into a tuple with three individual 
 *  elements by splitting the lines on commas. Now comes something new. Spark has a class Row
 *  and you need to import it in your script or program. Row comes from the same package as class 
 *  SQLContext.  Row class creates rows with named and typed fields.  You need to apply 
 *  “constructor” Row to every tuple in RDD emps_fields, like: 
 *  employees = emps_fields.map(lambda e: Row(name = e[0], age = int(e[1]), salary = float(e[2])))
 *
 * e[0], e[1] and e[2] are the first, second and third elements of the tuple e representing a
 * row (line) in RDD emps_fields. Note that int and float are types of fields in new rows.
 * Newly create RDD employees is now made of Row elements and is ready to be transformed into 
 * a DataFrame. You generate a DataFrame by passing an RDD of Row elements to
 * the method createDataFrame() of class SQLContext. Do it. Show the content of new DataFrame. 
 * Transform this DataFrame into a Temporary Table and select names of all
 * employees who have a salary greater than 3500. 
 *
 */

package hw6problem2; 

import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple3;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.DataFrame;

/**
 * The class hw6p1 make database operations with two text files using SPARK MapReduce technology
 */
public class hw6p2 {
   
 /** 
  * @param args[]  input and output paths and location of the file input and output files
 */
  public static void main(String[] args) throws Exception {
    String inputFile = args[0];
    String outputFile= args[1];
 /**
    * Create a Java Spark Context sc
  */
    
    SparkConf conf = new SparkConf().setAppName("hw6p2");
    JavaSparkContext sc = new JavaSparkContext(conf);
		
   SQLContext sqlContext = new SQLContext(sc);
  


/**
    *	Create RDD emps
    */
    JavaRDD<String> emps = sc.textFile(inputFile);
    

    /**
    *Turn into Tuple of words
    */

	JavaRDD<Tuple3<String, String, String>> emps_f = emps.map
          (
	      new Function<String,  Tuple3<String, String, String>>()
              {
	        public Tuple3<String, String,String> call(String x)
                { 
                 List L= Arrays.asList(x.toLowerCase().split( "\\W+") );
                return new  Tuple3(L.get(0), L.get(1),L.get(2) );

                // return new  Tuple3(x.split("\\W+")[0], x.split("\\W+")[1],x.split("\\W+")[2] );
                }


              }      
          );

       JavaRDD<Row> employees = emps_f.map
          (
              new Function< Tuple3<String, String, String> , Row>()
              {
                public Row   call(Tuple3<String, String,String> x)
                {
                      return  RowFactory.create( x._1(),  Integer.parseInt(x._2()), Float.parseFloat( x._3()));



                 }     
              }
           );


     StructType schema = new StructType(new StructField [] {
      new StructField("name", DataTypes.StringType, false, Metadata.empty()),
      new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("salary", DataTypes.FloatType, false, Metadata.empty())
       });

      DataFrame people = sqlContext.createDataFrame(employees, schema);

      people.show(3);

      sqlContext.registerDataFrameAsTable(people,"employee");
      DataFrame  df2 = sqlContext.table("employee");

      DataFrame sqltable = sqlContext.sql("select name from employee where salary >3500");

     sqltable.show();
 
     emps_f.saveAsTextFile(outputFile);

     sc.stop();
	}
}
