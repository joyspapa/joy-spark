package com.obzen.spark.batch.inykang;

import com.obzen.spark.batch.inykang.model.*;
import com.obzen.spark.batch.inykang.parser.ProductOrderParser;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author inykang
 */
public class ContentOrderBatchAppl implements Serializable {
    private static final Logger logger = Logger.getLogger(ContentOrderBatchAppl.class);
    private SparkConf sparkConf;

    /**
     * Main
     */
    public static void main(String... args) throws Exception {
        ContentOrderBatchAppl app = new ContentOrderBatchAppl();
        app.setUpSparkConfig(false);

        OptionParser parser = new OptionParser();
        //mandatory arguments
        OptionSpec<String> batchDateOp = parser.accepts("batchDate").withRequiredArg().ofType(String.class)
                .describedAs("batch date");
        OptionSpec<String> basePathOp = parser.accepts("baseDataPath").withRequiredArg().ofType(String.class)
                .describedAs("base data path");
        OptionSpec<String> orderPathOp = parser.accepts("orderDataPath").withRequiredArg().ofType(String.class)
                .describedAs("order data path");
        OptionSpec<String> outPathOp = parser.accepts("outDataPath").withRequiredArg().ofType(String.class)
                .describedAs("output data path");
        OptionSpec<String> inPathOp = parser.accepts("inDataPath").withRequiredArg().ofType(String.class)
                .describedAs("input data path");

        OptionSet options = parser.parse(args);
        if (!(options.has(basePathOp) && options.hasArgument(outPathOp)
                && options.hasArgument(orderPathOp) && options.hasArgument(batchDateOp)
                && options.hasArgument(inPathOp)
        )) {
            parser.printHelpOn(System.out);
            logger.error("▶ essential arguments are required!");
            System.exit(0);
        }

        app.start(options.valueOf(batchDateOp), options.valueOf(basePathOp)
                , options.valueOf(orderPathOp), options.valueOf(outPathOp)
                , options.valueOf(inPathOp)
        );
    }

    public void start(String date, String basePath, String orderPath, String outPath, String inPath) {
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.hadoopConfiguration().setBoolean("parquet.enable.summary-metadata", false);
        sc.hadoopConfiguration().set("parquet.metadata.read.parallelism", "10");
        sc.hadoopConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

        SQLContext sqlContext = new SQLContext(sc);
        ProductOrderParser parser = new ProductOrderParser();

        // 기본로그(Base)
        JavaPairRDD<String, Iterable<LogBaseDetail>> basePairRDD = sqlContext.read().format("parquet").load(basePath)
                .dropDuplicates()
                .javaRDD()
                .map(row -> parser.mapLogBaseDetail(row))
                .groupBy(baseDetail -> baseDetail.getSessionId());

        // 주문로그(Order)
        JavaRDD<OrgOrderLog> orderRDD = sqlContext.read().format("parquet").load(orderPath)
                .dropDuplicates()
                .javaRDD()
                .map(row -> parser.mapOrgOrderLog(row))
                //.groupBy(order -> order.getSessionId());
                ;

        // 상품정보
        Map<String, ProductInfo> productMap = new HashMap<>();
        sc.textFile(inPath + "/ProductInfo.csv")
                .map(parser::mapProductInfo)
                .collect()
                .forEach(info -> productMap.put(info.getProductCd(), info));

        // 고객별 상품별 주문집계(CustProductOrder)
//        JavaRDD<CustProductOrder> custProductOrderRDD =
//                orderRDD
//                        .groupBy(order -> order.getSessionId())
//                        .join(basePairRDD)
//                        .mapValues(tuple -> parser.mapCustProductOrder(date, tuple, productMap))
//                        .values()
//                        .flatMap(iter -> iter.iterator())
//                        .mapToPair(v -> new Tuple2<>(
//                                new Tuple3<>(v.getCust_id(), v.getCookie_id(), v.getProduct_cd())
//                                , v
//                        ))
//                        .reduceByKey((acc, curr) -> {
//                            acc.addOrder_cnt(curr.getOrder_cnt());
//                            acc.addAmount(curr.getAmount());
//                            return acc;
//                        })
//                        .values();
//        sqlContext.createDataFrame(custProductOrderRDD, CustProductOrder.class)
//                .write().mode(SaveMode.Overwrite).parquet(outPath + "/CustProductOrderDaily/dt=" + date);
//
//        // 상품별 주문요약(ProductOrderSummary)
//        JavaRDD<ProductOrderSummary> productOrderSummaryRDD =
//                custProductOrderRDD
//                        .map(parser::mapProductOrderSummary)
//                        .mapToPair(summ -> new Tuple2<>(summ.getProduct_cd(), summ))
//                        .reduceByKey((acc, curr) -> {
//                            acc.addOrder_cnt(curr.getOrder_cnt());
//                            acc.addAmount(curr.getAmount());
//                            return acc;
//                        })
//                        .values();
//        sqlContext.createDataFrame(productOrderSummaryRDD, ProductOrderSummary.class)
//                .write().mode(SaveMode.Overwrite).parquet(outPath + "/ProductOrderSummaryDaily/dt=" + date);

        // 컨텐츠별 주문요약(ContentOrderSummary)
        JavaRDD<ContentOrderSummary> contentOrderSummaryRDD =
                orderRDD
                        .mapToPair(order -> new Tuple2<>(
                                new Tuple2<>(order.getSessionId(), order.getProductCd())
                                , 1
                        ))
                        .reduceByKey((acc, count) -> acc + count)
                        .mapToPair(t -> new Tuple2<>(t._1()._2(), t._2()))
                        .reduceByKey((acc, count) -> acc + count)
                        .map(t -> parser.mapContentOrderSummary(date, productMap, t));

        contentOrderSummaryRDD.foreach(rdd -> logger.warn(String.format(
                "################ %s", rdd.toString()
        )));
        sqlContext.createDataFrame(contentOrderSummaryRDD, ContentOrderSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/ContentOrderSummaryDaily/dt=" + date);

    }

    public void setUpSparkConfig(boolean isLocal) {
        sparkConf = new SparkConf().setAppName("Daishin Realtime Data Batch");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class<?>[]{LogBaseDetail.class, OrgOrderLog.class});
    }

    private String buildKey(String... keys) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.length; i++)
            sb.append(convEmptyValue(keys[i]));

        return sb.toString();
    }

    private boolean isEmpty(String value) {
        return (value == null || value.trim().equals(""));
    }

    private static String convEmptyValue(String value) {
        if (value == null || value.trim().equalsIgnoreCase("null"))
            return "";
        else
            return value.trim();
    }
}