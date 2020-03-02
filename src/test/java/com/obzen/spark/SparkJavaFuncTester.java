package com.obzen.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SparkJavaFuncTester implements Serializable {
    private JavaSparkContext sc;
    private JavaPairRDD<String, Base> pairs;

    @Before
    public void setup() {
        SparkConf conf = new SparkConf()
                .setAppName("SparkJavaFuncTester")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Base b1 = new Base("A", 1, 1, 0, 0);
        Base b2 = new Base("B", 2, 1, 0, 0);
        Base b3 = new Base("C", 3, 1, 0, 0);
        Base b4 = new Base("B", 4, 1, 0, 0);
        Base b5 = new Base("A", 5, 1, 0, 0);
        Base b6 = new Base("A", 6, 1, 0, 0);
        Base b7 = new Base("A", 7, 1, 0, 0);

        List<Tuple2<String, Base>> baseList = Arrays.asList(
                new Tuple2<>(b1.id(), b1)
                , new Tuple2<>(b2.id(), b2)
                , new Tuple2<>(b3.id(), b3)
                , new Tuple2<>(b4.id(), b4)
                , new Tuple2<>(b5.id(), b5)
                , new Tuple2<>(b6.id(), b6)
                , new Tuple2<>(b7.id(), b7)
        );

        pairs = sc.parallelizePairs(baseList, 2);
    }

    @Test
    public void testAggregateByKey01() {
        List myList = Arrays.asList(
                new Tuple2<>("A", 8),
                new Tuple2<>("B", 3),
                new Tuple2<>("C", 5),
                new Tuple2<>("A", 2),
                new Tuple2<>("B", 8));

        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(myList);
        System.out.println(
                pairs
                        .aggregateByKey(new StatCounter()           // zeroValue
                                , (acc, x) -> acc.merge(x)          // seqFunc
                                , (acc1, acc2) -> acc1.merge(acc2)  // combFunc
                        )
                        //.map(x -> new Tuple3<>(x._1, x._2.mean(), x._2.stdev()))
                        .collect()
        );
    }


    @Test
    public void testReduceKey01() {
        List myList = Arrays.asList(
                new Tuple2<>("A", 8),
                new Tuple2<>("B", 3),
                new Tuple2<>("C", 5),
                new Tuple2<>("A", 2),
                new Tuple2<>("B", 8));

        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(myList);
        System.out.println(
                pairs
                        .reduceByKey((acc, curr) -> {
                    System.out.println(String.format(
                            "acc:%d, curr:%d", acc, curr
                    ));
                    return acc + curr;
                        })
//                        .aggregateByKey(new StatCounter()           // zeroValue
//                                , (acc, x) -> acc.merge(x)          // seqFunc
//                                , (acc1, acc2) -> acc1.merge(acc2)  // combFunc
//                        )
                        //.map(x -> new Tuple3<>(x._1, x._2.mean(), x._2.stdev()))
                        .collect()
        );
    }

    @Test
    public void testAggregateByKey02() {
        System.out.println(
                pairs
                        .aggregateByKey(new Base()           // zeroValue
                                // seqFunc
                                , (acc, x) -> {
                                    System.out.println(String.format(
                                            "acc:%s, x:%s"
                                            , acc.toString(), x.toString()
                                    ));
                                    acc.setId(x.id());
                                    acc.setValue(x.value());
                                    acc.addCount(x.count());
                                    acc.addSum(x.value());
                                    acc.setAvg(((double) acc.sum()) / (double) acc.count());
                                    return acc;
                                }
                                // combFunc
                                , (acc1, acc2) -> {
                                    System.out.println(String.format(
                                            "acc1:%s, acc2:%s"
                                            , acc1.toString(), acc2.toString()
                                    ));
                                    acc1.addCount(acc2.count());
                                    acc1.addSum(acc2.sum());
                                    acc1.setAvg((double) (acc1.sum()) / (double) acc1.count());
                                    return acc1;
                                }
                        )
                        //.map(x -> new Tuple3<>(x._1, x._2.mean(), x._2.stdev()))
                        .collect()
        );
    }

    @Test
    public void testAggregateByKey03() {
        System.out.println(
                pairs
                        .aggregateByKey(new Tuple2<>(new Base(), new HashSet<String>())           // zeroValue
                                // seqFunc
                                , (acc, x) -> {
                            Base base = acc._1();
                                    System.out.println(String.format(
                                            "acc:%s, x:%s"
                                            , acc.toString(), x.toString()
                                    ));
                                    base.setId(x.id());
                                    base.setValue(x.value());
                                    base.addCount(x.count());
                                    base.addSum(x.value());
                                    base.setAvg(((double) base.sum()) / (double) base.count());
                                    return acc;
                                }
                                // combFunc
                                , (acc1, acc2) -> {
                                    Base base1 = acc1._1();
                                    Base base2 = acc2._1();
                                    System.out.println(String.format(
                                            "acc1:%s, acc2:%s"
                                            , acc1.toString(), acc2.toString()
                                    ));
                                    base1.addCount(base2.count());
                                    base1.addSum(base2.sum());
                                    base1.setAvg((double) (base1.sum()) / (double) base1.count());
                                    acc1._2().addAll(acc2._2());
                                    return acc1;
                                }
                        )
                        //.map(x -> new Tuple3<>(x._1, x._2.mean(), x._2.stdev()))
                        .collect()
        );
    }

    @Test
    public void testFoldByKey() {
        System.out.println(
                pairs
                        .foldByKey(new Base(), (acc, x) -> {
                                    System.out.println(String.format(
                                            "acc:%s, x:%s"
                                            , acc.toString(), x.toString()
                                    ));
                                    acc.setId(x.id());
                                    acc.setValue(x.value());
                                    acc.addCount(x.count());
                                    acc.addSum(x.value());
                                    acc.setAvg(((double) acc.sum()) / (double) acc.count());
                                    return acc;
                                }
                        ).collect()
        );
    }

    @Test
    public void testReduceByKey() {
        System.out.println(
                pairs
                        .reduceByKey((acc, v) -> {
                                    System.out.println(String.format(
                                            "acc:%s, v:%s"
                                            , acc.toString(), v.toString()
                                    ));
                                    acc.setId(v.id());
                                    acc.setValue(v.value());
                                    acc.addCount(v.count());
                                    acc.addSum(v.sum());
                                    acc.setAvg(((double) acc.sum()) / (double) acc.count());
                                    return acc;
                                }
                        ).collect()
        );
    }
}