package kafka.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

/**
 * Write a consumer client that will replace the consumer from the previous problem. 
 *  However, rather than simply printing every message it receives from the producer, let it print 
 *  for us every 5 seconds the rolling count of numbers between 1 and 10 it received in the last 30 seconds. 
 * Usage: hw8p3 <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.hw8p3 broker1-host:port,broker2-host:port topic1,topic2
 */

public final class hw8p3 {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: hw8p3 <brokers> <topics>\n" +
          "  <brokers> is a list of one or more Kafka brokers\n" +
          "  <topics> is a list of one or more kafka topics to consume from\n\n");
      System.exit(1);
    }

    //StreamingExamples.setStreamingLogLevels();

    String brokers = args[0];
    String topics = args[1];

    // Create context with a 2 seconds batch interval
    //SparkConf sparkConf = new SparkConf().setAppName("hw8p3");
    JavaSparkContext sparkConf = new JavaSparkContext("local[5]", "hw8p3");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

    HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("metadata.broker.list", brokers);
    kafkaParams.put("zookeeper.connect", "localhost:2181");
    kafkaParams.put("group.id", "spark-app");
    System.out.println("Kafka parameters: " + kafkaParams);

    // Create direct kafka stream with brokers and topics
    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
        jssc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        topicsSet
    );

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      @Override
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }
    });
    
    
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
        return Arrays.asList(SPACE.split(x));
      }
    });
    
    
    JavaPairDStream<String, Integer> pairs = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      });

    
    // Reduce function adding two integers, defined separately for clarity
   Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
      @Override public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    };
    
    
    // Reduce last 30 seconds of data, every 5 seconds
    JavaPairDStream<String, Integer> windowedWordCounts = pairs.reduceByKeyAndWindow(
    		reduceFunc, Durations.seconds(30), Durations.seconds(5));
    
    windowedWordCounts.print();
 
    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}