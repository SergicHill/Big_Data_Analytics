/**
 *   Compares two text files using Spark on Java 
 *    @author Serguey Khovansky
 *    @version: March 2016
 *    This class  is a part of the solution to the homework 6, problem 1:
 *    This class flat splits the input text into words 
 *    using input regex
 *    It operates with Spark  JavaRDD 
 */
package hw6problem1;

import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

class mySplitter  implements Function<JavaRDD<String> ,JavaRDD<String>  > 
{    
     private String sregex;

/**
 *  Constructor
 * @param sregex  regex to split the text
 * */

      mySplitter(String sregex)
      {
        this.sregex = sregex;
      }

/**
 * Call function is implemented from Function class of Spark
 * @param input  JavaRDD<String>  object, obtained from input text file
 * @return words JavaRDD<String>  output object with words split  
 *
 * */
  @Override
    public  JavaRDD<String>  call( JavaRDD<String>  input) {
            JavaRDD<String>  words = input.flatMap
       (
          new FlatMapFunction<String, String>()
          {
              public Iterable<String> call(String x)
              {
                //return Arrays.asList(x.toLowerCase().split( "\\W+") );
                return Arrays.asList(x.toLowerCase().split(sregex));
               }
          }
        );
       return words;
    }
   }

