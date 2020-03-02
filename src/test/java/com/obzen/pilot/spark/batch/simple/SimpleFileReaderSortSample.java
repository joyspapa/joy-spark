package com.obzen.pilot.spark.batch.simple;

import com.obzen.pilot.spark.common.data.VenueInfo;
import junit.framework.Assert;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by hanmin on 16. 2. 1.
 */
public class SimpleFileReaderSortSample {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        reader("/home/hanmin/Documents/meetup/jsonfile", "");
    }

    /**
     * http://stackoverflow.com/questions/25362942/how-to-parsing-csv-or-json-file-with-apache-spark
     * http://beekeeperdata.com/posts/hadoop/2015/12/28/spark-java-tutorial.html
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

//        rdd_records = rdd_records.filter(new Function<VenueInfo, Boolean>() {
//            @Override
//            public Boolean call(VenueInfo v1) throws Exception {
//                return v1.getCountry().equals("ca");
//            }
//        });
//        sortBykeyExample(ctx);
//
//        sortBykeyExample1(ctx, rdd_records, startTime);

//        sortBykeyExample2(ctx, rdd_records, startTime);

        sortBykeyExample3(ctx, rdd_records, startTime);

    }

    /*
    (13,(Houston,us))
    (9,(New York,us))
    (6,(Cincinnati,us))
     */
    public static void sortBykeyExample3 (JavaSparkContext ctx, JavaRDD<VenueInfo> rdd_records, long startTime) {
        System.out.println("Elapsed rdd_records.mapToPair-Start-Time : ");


        JavaPairRDD<String, Tuple2<String, Integer>> records_JPRDD =
                rdd_records.mapToPair(new PairFunction<VenueInfo, String, Tuple2<String, Integer>>() {
                    public Tuple2<String, Tuple2<String, Integer>> call(VenueInfo record) {
                        Tuple2<String, Tuple2<String, Integer>> t2 =
                                new Tuple2<String, Tuple2<String, Integer>>(
                                        record.getCountry()+", "+record.getCity(),
                                        //record.getCountry(),
                                        new Tuple2<String, Integer>(record.getCity(), 1)
                                );
                        return t2;
                    }
                }); // (ca, Toronto,(Toronto,1))  - city가 "ca"인 경우이면서 reduceByKey 하기전,


        JavaPairRDD<String, Tuple2<String, Integer>> final_rdd_records =
                records_JPRDD.reduceByKey(new Function2<Tuple2<String, Integer>, Tuple2<String,Integer>, Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> v1,
                                                        Tuple2<String, Integer> v2) throws Exception {
                        //return new Tuple2<String, Integer>(v1._1 + v2._1, v1._2 + v2._2);  // (ca, Toronto,(TorontoToronto,2))
                        return new Tuple2<String, Integer>(v1._1 , v1._2 + v2._2);  // (ca, Toronto,(Toronto,2))
                    }
                }); // (ca, Edmonton,(Edmonton,1)) - city가 "ca"인 경우만, reduceByKey 한 후,


        JavaPairRDD<Integer, Tuple2<String, String>> records_JPRDD2 =
                final_rdd_records.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Integer>>, Integer, Tuple2<String, String>>() {
                    public Tuple2<Integer, Tuple2<String, String>> call(Tuple2<String, Tuple2<String, Integer>> v1) {
                        return new Tuple2<Integer, Tuple2<String, String>>(
                                        v1._2._2,
                                        new Tuple2<String, String>(v1._2._1, v1._1.split(",")[0])
                                );
                    }
                }); // (3,(Vancouver,ca))

        records_JPRDD2 = records_JPRDD2.sortByKey(false);  // 내림차순

//        JavaPairRDD<Tuple2<String, String>, Integer> records_JPRDD3 =
//                records_JPRDD2.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<String, String>>, Tuple2<String, String>, Integer>() {
//                    public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<Integer, Tuple2<String, String>> v1) {
//                        return new Tuple2<Tuple2<String, String>, Integer>(
//                                new Tuple2<String, String>(v1._2._2 , v1._2._1),
//                                v1._1
//                        );
//                    }
//                }); // ((ca,Vancouver),3)


        JavaPairRDD<Tuple2<String, String>, Integer> records_JPRDD3 =
                records_JPRDD2.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<String, String>>, Tuple2<String, String>, Integer>() {
                    public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<Integer, Tuple2<String, String>> v1) {
                        return v1.swap();
                    }
                });  // ((Vancouver,ca),3)



        records_JPRDD.saveAsTextFile("/working/testdata/final_sort_"+ startTime);



        /*
        첫번째가 Tuple 형태로 ((x, y ) ,z ) 일 경우 sortByKey 를 하게되면 컴파일 오류 혹은 Runtime 오류 발생 :
        java.lang.ClassCastException: scala.Tuple2 cannot be cast to java.lang.Comparable
         */
        //records_JPRDD3.sortByKey(false).saveAsTextFile("/working/testdata/final_sort_"+ startTime);

        ctx.stop();
    }

    /*
    (13,(Houston,1))
    (9,(New York,1))
    (6,(Cincinnati,1))
     */
    public static void sortBykeyExample2 (JavaSparkContext ctx, JavaRDD<VenueInfo> rdd_records, long startTime) {
        System.out.println("Elapsed rdd_records.mapToPair-Start-Time : ");


        JavaPairRDD<String, Tuple2<String, Integer>> records_JPRDD =
                rdd_records.mapToPair(new PairFunction<VenueInfo, String, Tuple2<String, Integer>>() {
                    public Tuple2<String, Tuple2<String, Integer>> call(VenueInfo record) {
                        Tuple2<String, Tuple2<String, Integer>> t2 =
                                new Tuple2<String, Tuple2<String, Integer>>(
                                        record.getCountry()+", "+record.getCity(),
                                        //record.getCountry(),
                                        new Tuple2<String, Integer>(record.getCity(), 1)
                                );
                        return t2;
                    }
                });


        JavaPairRDD<String, Tuple2<String, Integer>> final_rdd_records =
                records_JPRDD.reduceByKey(new Function2<Tuple2<String, Integer>, Tuple2<String,
                        Integer>, Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> v1,
                                                        Tuple2<String, Integer> v2) throws Exception {
                        //return new Tuple2<String, Integer>(v1._1 + v2._1, v1._2 + v2._2);  // (ca, Toronto,(TorontoToronto,2))
                        return new Tuple2<String, Integer>(v1._1 , v1._2 + v2._2);  // (ca, Toronto,(Toronto,2))
                    }
                });

        JavaPairRDD<Integer, Tuple2<String, Integer>> records_JPRDD2 =
                final_rdd_records.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Integer>>, Integer, Tuple2<String, Integer>>() {
                    public Tuple2<Integer, Tuple2<String, Integer>> call(Tuple2<String, Tuple2<String, Integer>> v1) {
                        Tuple2<Integer, Tuple2<String, Integer>> t2 =
                                new Tuple2<Integer, Tuple2<String, Integer>>(
                                        v1._2._2,
                                        //record.getCountry(),
                                        new Tuple2<String, Integer>(v1._2._1, 1)
                                );
                        return t2;
                    }
                });

        records_JPRDD2.sortByKey(false).saveAsTextFile("/working/testdata/final_sort_"+ startTime);

        ctx.stop();
    }

    /*
    (us, city,(city,1))
    (us, bloomington,(bloomington,1))
    (us, Winter Park,(Winter Park,1))
    (us, Winston-Salem,(Winston-Salem,1))
     */
    public static void sortBykeyExample1 (JavaSparkContext ctx, JavaRDD<VenueInfo> rdd_records, long startTime) {
        System.out.println("Elapsed rdd_records.mapToPair-Start-Time : ");


        JavaPairRDD<String, Tuple2<String, Integer>> records_JPRDD =
                rdd_records.mapToPair(new PairFunction<VenueInfo, String, Tuple2<String, Integer>>() {
                    public Tuple2<String, Tuple2<String, Integer>> call(VenueInfo record) {
                        Tuple2<String, Tuple2<String, Integer>> t2 =
                                new Tuple2<String, Tuple2<String, Integer>>(
                                        record.getCountry()+", "+record.getCity(),
                                        //record.getCountry(),
                                        new Tuple2<String, Integer>(record.getCity(), 1)
                                );
                        return t2;
                    }
                });

        System.out.println("Elapsed rdd_records.mapToPair-End-Time : ");

        System.out.println("middle result count: "+records_JPRDD.count());
        //records_JPRDD.saveAsTextFile("/working/testdata/middle_"+ startTime);


        JavaPairRDD<String, Tuple2<String, Integer>> final_rdd_records =
                records_JPRDD.reduceByKey(new Function2<Tuple2<String, Integer>, Tuple2<String,
                        Integer>, Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> v1,
                                                        Tuple2<String, Integer> v2) throws Exception {
                        //return new Tuple2<String, Integer>(v1._1 + v2._1, v1._2 + v2._2);  // (ca, Toronto,(TorontoToronto,2))
                        return new Tuple2<String, Integer>(v1._1 , v1._2 + v2._2);  // (ca, Toronto,(Toronto,2))
                    }
                });



        //List<Tuple2<Tuple2<Integer, String>, Integer>> wordSortedByCount = records_JPRDD.sortByKey(new TupleComparator(), false).collect();

        System.out.println("final result count: "+final_rdd_records.count());
        final_rdd_records.sortByKey(false).saveAsTextFile("/working/testdata/final_"+ startTime);


        System.out.println("Elapsed Total-Time : "+(System.currentTimeMillis()-startTime)+"ms");

        ctx.stop();
    }

    /**
     * http://www.programcreek.com/java-api-examples/index.php?api=org.apache.spark.api.java.JavaPairRDD
     * @param ctx
     */
    public static void sortBykeyExample (JavaSparkContext ctx) {
        List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
        pairs.add(new Tuple2<>(0, 4));
        pairs.add(new Tuple2<>(3, 2));
        pairs.add(new Tuple2<>(-1, 1));

        JavaPairRDD<Integer, Integer> rdd = ctx.parallelizePairs(pairs);

        // Default comparator
        JavaPairRDD<Integer, Integer> sortedRDD = rdd.sortByKey();
        Assert.assertEquals(new Tuple2<>(-1, 1), sortedRDD.first());
        List<Tuple2<Integer, Integer>> sortedPairs = sortedRDD.collect();
        System.out.println("==================1======================>"+sortedPairs);
        Assert.assertEquals(new Tuple2<>(0, 4), sortedPairs.get(1));
        Assert.assertEquals(new Tuple2<>(3, 2), sortedPairs.get(2));

        // Custom comparator
        sortedRDD = rdd.sortByKey(Collections.<Integer>reverseOrder(), false);
        Assert.assertEquals(new Tuple2<>(-1, 1), sortedRDD.first());
        sortedPairs = sortedRDD.collect();
        Assert.assertEquals(new Tuple2<>(0, 4), sortedPairs.get(1));
        Assert.assertEquals(new Tuple2<>(3, 2), sortedPairs.get(2));



        System.out.println("===================2=====================>"+sortedPairs);

        List<Tuple2<Integer, Integer>> pairs2 = rdd.sortByKey(false).take(3);
        pairs2.forEach(t->System.out.println(t));
    }
}
