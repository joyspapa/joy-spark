package com.obzen.pilot.spark.batch.simple;

import com.obzen.pilot.spark.common.data.VenueInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.util.regex.Pattern;

/**
 * Created by hanmin on 16. 2. 1.
 */
public class SimpleWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        reader("/home/hanmin/Documents/meetup/jsonfile", "");
    }

    /**
     *
     * @param filename
     * @param demoType
     */
    public static void reader(String filename, String demoType) {

        // logging
        StringBuilder builder = new StringBuilder(100);
        builder.append("WordCount Application Start type is : ").append(demoType).append(" [").append(LocalDateTime.now()).append("]");
        System.out.println(builder.toString());
        long startTime = System.currentTimeMillis();


//        SparkConf sparkConf = new SparkConf().setAppName("WordCountDemo");
        SparkConf sparkConf = new SparkConf().setAppName("WordCountDemo").setMaster("local[2]");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        System.out.println("Elapsed create.JavaSparkContext-Time : "+(System.currentTimeMillis()-startTime)+"ms");

        JavaRDD<String> lines = ctx.textFile(filename, 1);
        System.out.println("Elapsed ctx.textFile-Time : "+(System.currentTimeMillis()-startTime)+"ms");

        JavaRDD<VenueInfo> rdd_records = lines.map(
                new Function<String, VenueInfo>() {
                    public VenueInfo call(String line) throws Exception {
                        // Here you can use JSON
                        // Gson gson = new Gson();
                        // gson.fromJson(line, Record.class);
                        VenueInfo sd = new VenueInfo(line);
                        return sd;
                    }
                });

        System.out.println("Elapsed rdd_records.mapToPair-Start-Time : ");

        JavaPairRDD<String, Tuple2<String, Integer>> records_JPRDD =
                rdd_records.mapToPair(new PairFunction<VenueInfo, String, Tuple2<String, Integer>>() {
                                                  public Tuple2<String, Tuple2<String, Integer>> call(VenueInfo record) {
                                                      Tuple2<String, Tuple2<String, Integer>> t2 =
                                                              new Tuple2<String, Tuple2<String, Integer>>(
                                                                      //record.getCountry()+", "+record.getCity(),
                                                                      record.getCountry(),
                                                                      new Tuple2<String, Integer>(record.getCity(), 1)
                                                              );
                                                      return t2;
                                                  }
                                              });

        /*
        * Key가 이것만 있을 경우 record.getCountry()
        (nl,(Amsterdam,1))
        (us,(Bakersfield,136))
        (ca,(Toronto,7))
        (gb,(London,2))
        (mx,(Mexico City,1))
        (au,(Sydney,3))
        (fr,(Paris,1))

        * Key가 record.getCountry()+", "+record.getCity()
        (us, La Mesa,(La Mesa,1))
        (us, Evanston,(Evanston,1))
        (us, Cincinnati,(Cincinnati,6))
        (us, Lakewood,(Lakewood,1))
        (nl, Amsterdam,(Amsterdam,1))
        (us, Houston,(Houston,13))
        (us, Parsippany,(Parsippany,1))
        ...
         */
        System.out.println("Elapsed rdd_records.mapToPair-End-Time : ");

        System.out.println("middle result count: "+records_JPRDD.count());
        records_JPRDD.saveAsTextFile("/working/testdata/middle_"+ startTime);

//        JavaPairRDD<String, Tuple2<String, Integer>> final_rdd_records_imsi =
//                records_JPRDD.reduceByKey(new Function2<String, Tuple2<String,
//                        Integer>, Tuple2<String, Integer>>() {
//                    public Tuple2<String, Integer> call(String v1,
//                                                        Tuple2<String, Integer> v2) throws Exception {
//                        return new Tuple2<String, Integer>(v1 ,v1 + v2._1);
//                    }
//                });
//return new Tuple2<String, Integer>(v2._1, v2._2+v2._2);

//        JavaPairRDD<String, Tuple2<String, Integer>> final_rdd_records =
//                records_JPRDD.reduceByKey(new Function2<Tuple2<String, Integer>, Tuple2<String,
//                        Integer>, Tuple2<String, Integer>>() {
//                    public Tuple2<String, Integer> call(Tuple2<String, Integer> v1,
//                                                        Tuple2<String, Integer> v2) throws Exception {
//                        return new Tuple2<String, Integer>(v2._1, v2._2+v2._2);
//                    }
//                });

        JavaPairRDD<String, Tuple2<String, Integer>> final_rdd_records =
                records_JPRDD.reduceByKey(new Function2<Tuple2<String, Integer>, Tuple2<String,
                                        Integer>, Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> v1,
                                                      Tuple2<String, Integer> v2) throws Exception {
                        //return new Tuple2<String, Integer>(v1._1 + v2._1, v1._2 + v2._2);  // (ca, Toronto,(TorontoToronto,2))
                        return new Tuple2<String, Integer>(v1._1 , v1._2 + v2._2);  // (ca, Toronto,(Toronto,2))
                    }
                });


        System.out.println("final result count: "+final_rdd_records.count());
        final_rdd_records.saveAsTextFile("/working/testdata/final_"+ startTime);

//        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)));
//        System.out.println("Elapsed lines.flatMap-Time : "+(System.currentTimeMillis()-startTime)+"ms");

//        JavaPairRDD<String, Integer> ones = rdd_records.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
//        System.out.println("Elapsed words.mapToPair-Time : "+(System.currentTimeMillis()-startTime)+"ms");

//        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
//        System.out.println("Elapsed ones.reduceByKey-Time : "+(System.currentTimeMillis()-startTime)+"ms");


//        if(demoType.equals("0")) {
//
//            System.out.println(filename+" file's line count : "+counts.count());
//
//        } else if(demoType.equals("1")) {
//
//            String tag = "./result/"+ startTime; // local test "hdfs://localhost:9000/user/hduser/"
//            System.out.println("File name written on hdfs is : " + tag);
//            counts.saveAsTextFile(tag);
//
//        } else {
//
//            List<Tuple2<String, Integer>> output = counts.collect();
//            System.out.println("Elapsed counts.collect-Time : " + (System.currentTimeMillis() - startTime)+"ms");
//            for (Tuple2<?, ?> tuple : output) {
//                System.out.println(tuple._1() + ": " + tuple._2());
//            }
//
//        }

        System.out.println("Elapsed Total-Time : "+(System.currentTimeMillis()-startTime)+"ms");

        ctx.stop();
    }
}
