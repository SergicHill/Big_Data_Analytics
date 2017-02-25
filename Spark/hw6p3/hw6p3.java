/**
 *  @author Serguey Khovansky
 *  @version: March 2016
 * This is solution of the homework 6, problem 3:
 *
 *  Attached file ebay.csv contains information of eBay’s auction history. 
 *  The Excel file has 9 columns and they represent:
 * The eBay online auction dataset has the following data fields:
 * •	auctionid - unique identifier of an auction
 * •	bid - the proxy bid placed by a bidder
 * •	bidtime - the time (in days) that the bid was placed, from the start of the auction
 * •	bidder - eBay username of the bidder
 * •	bidderrate - eBay feedback rating of the bidder
 * •	openbid - the opening bid set by the seller
 * •	price - the closing price that the item sold for (equivalent to
 *               the second highest bid + an increment)
 * •	item – name of the item being sold
 * •	daystolive – length of the auction.
 *
 * Using Spark DataFrames you will explore the data with following 4 questions:
 * 1.	How many auctions were held?
 * 2.	How many bids were made per item?
 * 3.	
 * o	What's the minimum, maximum, and average bid (price) per item?
 * o	What is the minimum, maximum and average number of bids per item?
 * 4.	Show the bids with price > 100
 * Import data into an RDD object. Transform that RDD into an RDD of Row-s by assign schema 
 * (column names and types). Transform that new RDD into a DataFrame. Call that DataFrame Auction.
 *  Show (print) the schema of the DataFrame. Make above queries using DatFrame API. 
 *  You recall how we applied methods: select(), groupBy(), count() and others to the DataFrame
 *   in class. Use those methods.
 * Next transform your Auction DataFrame into a table and make the same 4 inquiries 
 * using regular SQL queries. 
 *
 */

package hw6problem3; 

import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple9;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.DataFrame;

/**
 * The class hw6p1 make database operations with two text files using SPARK MapReduce technology
 */
public class hw6p3 {
   
 /** 
  * @param args[]  input and output paths and location of the file input and output files
 */
  public static void main(String[] args) throws Exception {
    String inputFile = args[0];
//    String outputFile= args[1];

 /**
    * Create a Java Spark Context sc
  */
    
    SparkConf conf = new SparkConf().setAppName("hw6p3");
    JavaSparkContext sc = new JavaSparkContext(conf);
		
    SQLContext sqlContext = new SQLContext(sc);
  


/**
    *	Create RDD emps
    */
    JavaRDD<String> ebay = sc.textFile(inputFile);
    

    /**
    *Turn into Tuple of words
    */

	JavaRDD<Tuple9<String, String, String,  String, String, String, String, String, String>> ebay_rdd_tuple9 = ebay.map
           (
           new Function<String,  Tuple9<String, String, String,String, String, String, String, String, String>>()
	      {
	        public Tuple9<String, String,String,String, String, String, String, String, String > call(String x)
                { 
                 List L= Arrays.asList(x.toLowerCase().split( "\\,") );
                 return new  Tuple9(L.get(0), L.get(1),L.get(2), L.get(3),L.get(4),L.get(5),L.get(6),L.get(7),L.get(8) );
                }
              }      
          );

       JavaRDD<Row> ebay_row = ebay_rdd_tuple9.map
          (
          new Function< Tuple9<String, String, String,String, String, String, String, String, String  > , Row>()
              {
    public Row   call(Tuple9<String, String,String, String, String, String, String, String, String> x)

                {
   return  RowFactory.create( Long.parseLong(x._1()), Float.parseFloat( x._2()) ,Float.parseFloat(x._3()), x._4(),  Integer.parseInt(x._5()), Float.parseFloat( x._6()),Float.parseFloat( x._7()), x._8().toString(),Integer.parseInt( x._9())  );


                 }     
              }
           );



     StructType schema = new StructType(new StructField [] {
      new StructField("auctionid", DataTypes.LongType, false, Metadata.empty()),
      new StructField("bid", DataTypes.FloatType, false, Metadata.empty()),
      new StructField("bidtime", DataTypes.FloatType, false, Metadata.empty()),

      new StructField("bidder", DataTypes.StringType, false, Metadata.empty()),
      new StructField("bidderrate", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("openbid", DataTypes.FloatType, false, Metadata.empty()),
      new StructField("price", DataTypes.FloatType, false, Metadata.empty()),
      new StructField("item", DataTypes.StringType, false, Metadata.empty()),
      new StructField("daystolive", DataTypes.IntegerType, false, Metadata.empty())

       });

      DataFrame dfauction = sqlContext.createDataFrame(ebay_row, schema);

      dfauction.printSchema();


     System.out.println("Total number of auctionid = "+ dfauction.select("auctionid").count());

    dfauction.groupBy("item").count().show();

// What's the minimum, maximum, and average bid (price) per item?
      dfauction.groupBy("item").min("price").show();
      dfauction.groupBy("item").max("price").show();
      dfauction.groupBy("item").mean("price").show();

// What is the minimum, maximum and average number of bids per item?
     
 dfauction.groupBy("item").count().describe().show();


//Show the bids with price > 100

     dfauction.filter("price>100").select("bid","price","item").show(10);

//Next transform your Auction DataFrame into a table


      sqlContext.registerDataFrameAsTable(dfauction,"ebay");
      DataFrame  df_ebay = sqlContext.table("ebay");

      DataFrame sqltable = sqlContext.sql("select * from ebay where price >200 ");
      sqltable.show(10);

      // how many auctions
      sqlContext.sql("select count(auctionid) from ebay ").show(10);

      // how many bids
      sqlContext.sql("select item, count(*) from ebay group by item").show(10);

 // What's the minimum, maximum, and average bid (price) per item?
       sqlContext.sql("select item, min(price) from ebay group by item").show(10);
       sqlContext.sql("select item, max(price) from ebay group by item").show(10);
       sqlContext.sql("select item, avg(price) from ebay group by item").show(10);


//What is the minimum, maximum and average number of bids per item?
   sqlContext.sql("select min(cnt) from (select item, count(*) as cnt from ebay group by item) as t1").show(10);
   sqlContext.sql("select max(cnt) from (select item, count(*) as cnt from ebay group by item) as t1").show(10);
   sqlContext.sql("select avg(cnt) from (select item, count(*) as cnt from ebay group by item) as t1").show(10);



//Show the bids with price > 100
    sqlContext.sql("select bid from ebay  where price>100").show(10);



//parquet
   sqltable.write().parquet("dfebay.parquet");
   
// Read in the parquet file created above.
// Parquet files are self-describing so the schema is preserved.
// The result of loading a parquet file is also a DataFrame.
   
 DataFrame parquetFile = sqlContext.read().parquet("dfebay.parquet");

 parquetFile.registerTempTable("parquetFile");
//    Dataset<Row> tmp = 
System.out.println("Now select from parquet File");
sqlContext.sql("SELECT item FROM parquetFile LIMIT 10 ").show(10);

     sc.stop();
	}
}

