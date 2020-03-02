package com.obzen.spark.batch.inykang;

import com.obzen.spark.batch.inykang.model.*;
import com.obzen.spark.batch.inykang.parser.ObzLogDetailParserOld;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author inykang
 */
public class LogDetailBatchApplOld implements Serializable {
    private static final Logger logger = Logger.getLogger(LogDetailBatchApplOld.class);
    private SparkConf sparkConf;

    /**
     * Main
     */
    public static void main(String... args) throws Exception {
        LogDetailBatchApplOld app = new LogDetailBatchApplOld();
        app.setUpSparkConfig(false);

        OptionParser parser = new OptionParser();
        //mandatory arguments
        OptionSpec<String> batchDateOp = parser.accepts("batchDate").withRequiredArg().ofType(String.class)
                .describedAs("batch date");
        OptionSpec<String> basePathOp = parser.accepts("baseDataPath").withRequiredArg().ofType(String.class)
                .describedAs("base data path");
        OptionSpec<String> ordPathOp = parser.accepts("ordDataPath").withRequiredArg().ofType(String.class)
                .describedAs("order data path");
        OptionSpec<String> outPathOp = parser.accepts("outDataPath").withRequiredArg().ofType(String.class)
                .describedAs("output data path");
        OptionSpec<String> inPathOp = parser.accepts("inDataPath").withRequiredArg().ofType(String.class)
                .describedAs("input data path");

        OptionSet options = parser.parse(args);
        if (!(options.has(basePathOp) && options.hasArgument(outPathOp)
                && options.hasArgument(ordPathOp) && options.hasArgument(batchDateOp)
                && options.hasArgument(inPathOp)
        )) {
            parser.printHelpOn(System.out);
            logger.error("▶ essential arguments are required!");
            System.exit(0);
        }

        app.start(options.valueOf(batchDateOp), options.valueOf(basePathOp)
                , options.valueOf(ordPathOp), options.valueOf(outPathOp)
                , options.valueOf(inPathOp)
        );
    }

    public void start(String date, String basePath, String ordPath, String outPath, String inPath) {
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.hadoopConfiguration().setBoolean("parquet.enable.summary-metadata", false);
        sc.hadoopConfiguration().set("parquet.metadata.read.parallelism", "10");
        sc.hadoopConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

        SQLContext sqlContext = new SQLContext(sc);
        ObzLogDetailParserOld parser = new ObzLogDetailParserOld();

        JavaRDD<Row> baseRDD = sqlContext.read().format("parquet").load(basePath)
                .javaRDD()
                //.persist(StorageLevel.MEMORY_AND_DISK_SER())
                ;

        // 주문로그(Order)
        JavaPairRDD<String, Iterable<OrgOrderLog>> orderRDD = sqlContext.read().format("parquet").load(ordPath)
                .javaRDD()
                .map(row -> parser.mapOrgOrderLog(row))
                .groupBy(order -> order.getSessionId());

        // 로그상세
        JavaRDD<LogBaseDetail> baseDetailRDD =
                baseRDD
                        .map(row -> parser.mapLogBaseDetail(row))
                        .groupBy(baseDetail -> baseDetail.getSessionId())
                        //.join(orderRDD)
                        //.mapValues(tuple -> parser.sortAndCalcStayingTime(tuple))
                        .leftOuterJoin(orderRDD)
                        .mapValues(parser::sortAndCalcStayingTime)
                        .values()
                        .flatMap(iter -> iter.iterator());
        sqlContext.createDataFrame(baseDetailRDD, LogBaseDetail.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/LogDetail" + "/" + date);


        // 방문상세
        JavaRDD<VisitDetail> visitDetailRDD =
                baseDetailRDD
                        .groupBy(base -> base.getSessionId())
                        .mapValues(base -> parser.mapVisitDetail(date, base))
                        .values();
        sqlContext.createDataFrame(visitDetailRDD, VisitDetail.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/VisitDetail" + "/" + date);

        // 고객방문집계(일별)
        JavaRDD<CustVisitDaily> custVisitDailyRDD =
                baseDetailRDD
                        .groupBy(base -> base.getCustId())
                        .mapValues(base -> parser.mapCustVisitDaily(date, base))
                        .values();
        sqlContext.createDataFrame(custVisitDailyRDD, CustVisitDaily.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/CustVisitDaily" + "/" + date);

        // 고객속성방문요약(일별)
        JavaRDD<CustVisitSummary> custVisitSummaryDailyRDD =
                baseDetailRDD
                        .groupBy(base -> buildStringKey(
                                base.getSex(), base.getAge(), base.getPlace(), base.getCustId()
                        ))
                        .mapValues(base -> parser.mapCustVisitSummaryDaily(date, base))
                        .values();
        sqlContext.createDataFrame(custVisitSummaryDailyRDD, CustVisitSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/CustVisitSummary" + "/" + date);

        // 디바이스별방문요약(일별)
        JavaRDD<DeviceVisitSummary> deviceVisitSummaryDailyRDD =
                baseDetailRDD
                        .groupBy(base -> buildStringKey(
                                base.getDeviceType(), base.getAppWebType()
                        ))
                        .mapValues(base -> parser.mapDeviceVisitSummaryDaily(date, base))
                        .values();
        sqlContext.createDataFrame(deviceVisitSummaryDailyRDD, DeviceVisitSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/DeviceVisitSummary" + "/" + date);

        // 유입채널별방문요약(일별)
        JavaRDD<InChnlVisitSummary> inChnlVisitSummaryDailyRDD =
                baseDetailRDD
                        .groupBy(base -> buildStringKey(
                                base.getInboundChnlSrc(), base.getInboundChnlMedium()
                                , base.getInboundChnlMedium(), base.getInboundChnlKeyword()
                        ))
                        .mapValues(base -> parser.mapInChnlVisitSummaryDaily(date, base))
                        .values();
        sqlContext.createDataFrame(inChnlVisitSummaryDailyRDD, InChnlVisitSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/InChnlVisitSummary" + "/" + date);

        // 고객별검색어별집계(일별)
        JavaRDD<CustSearchword> custSearchwordDailyRDD =
                baseDetailRDD
                        .filter(base -> !isEmpty(base.getSearchWord()))
                        .groupBy(base -> buildStringKey(
                                base.getCustId(), base.getCookieId(), base.getSearchWord()
                        ))
                        .mapValues(base -> parser.mapCustSearchwordDaily(date, base))
                        .values();
        sqlContext.createDataFrame(custSearchwordDailyRDD, CustSearchword.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/CustSearchword" + "/" + date);

        // 검색어별요약(일별)
        JavaRDD<SearchwordSummary> searchwordSummaryDailyRDD =
                custSearchwordDailyRDD
                        .mapToPair(searchwd -> new Tuple2<>(searchwd.getSearchword()
                                , parser.mapSearchwordSummaryDaily(searchwd)))
                        .reduceByKey(parser::reduceSearchwordSummaryDaily)
                        .values();
        sqlContext.createDataFrame(searchwordSummaryDailyRDD, SearchwordSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/SearchwordSummary" + "/" + date);

        // 상품, 고객
        JavaRDD<ContentVisit> contentVisitRDD =
                baseDetailRDD
                        .groupBy(base -> base.getSessionId())
                        .filter(parser::hasProductCode)
                        .mapValues(base -> parser.mapContentVisit(base))
                        .values()
                        .flatMap(iter -> iter.iterator());
        // 컨텐츠별방문요약(일별)
        Map<String, ProductInfo> prodMap = new HashMap<>();
        sc.textFile(inPath + "/ProductInfo.csv")
                .map(parser::mapProductInfo)
                .collect()
                .forEach(info -> prodMap.put(info.getProductCd(), info));

        //JavaRDD<ContentVisitSummary> contentVisitSummaryRDD =
                contentVisitRDD
                        .mapToPair(content ->
                                new Tuple2<>(content.getProductCd(), content))
//                        .aggregateByKey(new ContentVisitSummary(), (c1, c2) -> {
//
//                        })
                ;

        //sqlContext.createDataFrame(contentVisitSummaryRDD, ContentVisitSummary.class)
        //        .write().mode(SaveMode.Overwrite).parquet(outPath + "/ContentVisitSummary" + "/" + date);


    }

    public void setUpSparkConfig(boolean isLocal) {
        sparkConf = new SparkConf().setAppName("Daishin Realtime Data Batch");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class<?>[]{LogBaseDetail.class, OrgOrderLog.class});
    }

    private String buildStringKey(String... keys) {
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