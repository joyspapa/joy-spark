package com.obzen.spark.batch.inykang;

import com.obzen.spark.batch.inykang.model.*;
import com.obzen.spark.batch.inykang.parser.LogDetailParser;

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
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * @author inykang
 */
public class LogDetailBatchDFAppl implements Serializable {
    private static final Logger logger = Logger.getLogger(LogDetailBatchDFAppl.class);
    private SparkConf sparkConf;

    /**
     * Main
     */
    public static void main(String... args) throws Exception {
        LogDetailBatchDFAppl app = new LogDetailBatchDFAppl();
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
        LogDetailParser parser = new LogDetailParser();

        // 주문로그(Order)
        JavaPairRDD<String, Iterable<OrgOrderLog>> orderRDD = sqlContext.read().format("parquet").load(ordPath)
                //.dropDuplicates()
                .javaRDD()
                .map(row -> parser.mapOrgOrderLog(row))
                .groupBy(order -> order.getSessionId())
                .repartition(6)
                .persist(StorageLevel.MEMORY_ONLY())
                ;

        // 로그상세(BaseDetail)
        JavaRDD<LogBaseDetail> baseDetailRDD = sqlContext.read().format("parquet").load(basePath)
                //.dropDuplicates()
                .javaRDD()
                .map(row -> parser.mapLogBaseDetail(row))
                .groupBy(baseDetail -> baseDetail.getSessionId())
                .repartition(6)
                .leftOuterJoin(orderRDD)
                .mapValues(parser::sortAndCalcStayingTime)
                .values()
                .flatMap(iter -> iter.iterator())
                .persist(StorageLevel.MEMORY_ONLY());
        sqlContext.createDataFrame(baseDetailRDD, LogBaseDetail.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/LogDetail" + "/dt=" + date);


        // 방문상세(VisitDetail)
        JavaRDD<VisitDetail> visitDetailRDD =
                baseDetailRDD
                        .groupBy(base -> base.getSessionId())
                        .mapValues(base -> parser.mapVisitDetail(date, base))
                        .values()
                        .persist(StorageLevel.MEMORY_ONLY());
        sqlContext.createDataFrame(visitDetailRDD, VisitDetail.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/VisitDetail" + "/dt=" + date);

        // 고객방문집계(일별)(CustVisitDaily)
        JavaRDD<CustVisitDaily> custVisitDailyRDD =
                visitDetailRDD
                        .mapToPair(parser::groupingCustVisit)
                        .reduceByKey(parser::sumCustVisitDaily)
                        .values();
        sqlContext.createDataFrame(custVisitDailyRDD, CustVisitDaily.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/CustVisitDaily" + "/dt=" + date);

        // VisitSummary
        JavaRDD<VisitSummary> visitSummaryRDD =
                visitDetailRDD
                        .map(parser::mapVisitSummary);

        // 고객속성별 방문요약(일별)(CustVisitSummary)
        JavaRDD<CustVisitSummary> custVisitSummaryDailyRDD =
                visitSummaryRDD
                        .mapToPair(visit -> new Tuple2<>(
                                buildKey(visit.getSex(), visit.getAge()
                                        , visit.getPlace(), visit.getCust_grade())
                                , visit
                        ))
                        .aggregateByKey(new VisitSummary() // initialize
                                , parser::seqSumVisitSummary // sequence func.
                                , parser::combineSumVisitSummary // combine func.
                        )
                        .values()
                        .map(parser::mapCustVisitSummary);
        sqlContext.createDataFrame(custVisitSummaryDailyRDD, CustVisitSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/CustVisitSummaryDaily" + "/dt=" + date);


        // 디바이스별 방문요약(일별)(DeviceVisitSummary)
        JavaRDD<DeviceVisitSummary> deviceVisitSummaryRDD =
                visitSummaryRDD
                        .mapToPair(visit -> new Tuple2<>(
                                buildKey(visit.getDevice_type(), visit.getApp_web_type())
                                , visit
                        ))
                        .aggregateByKey(new VisitSummary() //initialize
                                , parser::seqSumVisitSummary // sequence func.
                                , parser::combineSumVisitSummary // combine func.
                        )
                        .values()
                        .map(parser::mapDeviceVisitSummary);
        sqlContext.createDataFrame(deviceVisitSummaryRDD, DeviceVisitSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/DeviceVisitSummaryDaily" + "/dt=" + date);

        // 유입채널별 방문요약(일별)(InChnlVisitSummary)
        JavaRDD<InChnlVisitSummary> inChnlVisitSummaryDailyRDD =
                visitSummaryRDD
                        .mapToPair(visit -> new Tuple2<>(
                                buildKey(visit.getInbound_chnl_src(), visit.getInbound_chnl_medium()
                                        , visit.getInbound_chnl_campaign(), visit.getInbound_chnl_keyword())
                                , visit)
                        )
                        .aggregateByKey(new VisitSummary() //initialize
                                , parser::seqSumVisitSummary // sequence func.
                                , parser::combineSumVisitSummary // combine func.
                        )
                        .values()
                        .map(parser::mapInChnlVisitSummary);
//                visitDetailRDD
//                        .groupBy(visit -> buildKey(
//                                visit.getInbound_chnl_src(), visit.getInbound_chnl_medium()
//                                , visit.getInbound_chnl_medium(), visit.getInbound_chnl_keyword()
//                        ))
//                        .mapValues(visit -> parser.mapInChnlVisitSummary(visit))
//                        .values();
        sqlContext.createDataFrame(inChnlVisitSummaryDailyRDD, InChnlVisitSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/InChnlVisitSummaryDaily" + "/dt=" + date);

        // 고객별 검색어별 집계(일별)(CustSearchword)
        JavaRDD<CustSearchword> custSearchwordRDD =
                baseDetailRDD
                        .filter(base -> !isEmpty(base.getSearchWord()))
                        //.map(base -> parser.mapCustSearchwordDaily(date, base))
                        .mapToPair(base -> new Tuple2<>(
                                buildKey(base.getCustId(), base.getCookieId(), base.getSearchWord())
                                , parser.mapCustSearchwordDaily(date, base)
                        ))
                        .reduceByKey(parser::sumCustSearchword)
                        .values();

/*
                        .groupBy(base -> buildKey(
                                base.getCustId(), base.getCookieId(), base.getSearchWord()
                        ))
                        .mapValues(base -> parser.mapCustSearchwordDaily(date, base))
                        .values();
*/
        sqlContext.createDataFrame(custSearchwordRDD, CustSearchword.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/CustSearchwordDaily" + "/dt=" + date);

        // 검색어별요약(일별)(SearchwordSummary)
        JavaRDD<SearchwordSummary> searchwordSummaryRDD =
                custSearchwordRDD
                        .mapToPair(searchwd -> new Tuple2<>(searchwd.getSearchword()
                                , parser.mapSearchwordSummaryDaily(searchwd)))
                        .reduceByKey(parser::reduceSearchwordSummaryDaily)
                        .values();
        sqlContext.createDataFrame(searchwordSummaryRDD, SearchwordSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/SearchwordSummaryDaily" + "/dt=" + date);

        // 시나리오경로 방문목록(일별)(ScenarioPathVisit)
        JavaRDD<ScenarioPathVisit> scenarioPathVisittRDD =
                baseDetailRDD
                        .filter(base -> parser.isScenarioPage(base.getPvType()))
                        .mapToPair(base ->
                                new Tuple2<>(new Tuple2<>(base.getSessionId(), base.getPvType())
                                        , parser.mapScenarioPathVisit(date, base))
                        )
                        .reduceByKey(parser::mergeScenarioPathVisit)
                        .values()
                //.map(tuple -> tuple._2())
                ;
        sqlContext.createDataFrame(scenarioPathVisittRDD, ScenarioPathVisit.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/ScenarioPathVisit" + "/dt=" + date);

        // 유입채널별 시나리오경로별 방문요약(일별)(InChnlScenarioPathSummary)
        JavaRDD<InChnlScenarioPathSummary> inChnlScenarioPathSummaryRDD =
                scenarioPathVisittRDD
                        .mapToPair(path -> new Tuple2<>(
                                buildKey(path.getPv_type(), path.getInbound_chnl_src(), path.getInbound_chnl_medium()
                                        , path.getInbound_chnl_campaign())
                                , parser.mapInChnlScenarioPathSummary(path))
                        )
                        .reduceByKey(parser::sumInChnlScenarioPathSummary)
                        .values();
        sqlContext.createDataFrame(inChnlScenarioPathSummaryRDD, InChnlScenarioPathSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/InChnlScenarioPathSummaryDaily" + "/dt=" + date);

        // 상품정보
        Map<String, ProductInfo> prodInfoMap = new HashMap<>();
        sc.textFile(inPath + "/ProductInfo.csv")
                .map(parser::mapProductInfo)
                .collect()
                .forEach(info -> prodInfoMap.put(info.getProductCd(), info));

        // 상품 디스플레이 정보
        Map<String, ProductDisplayInfo> prodDsplInfoMap = new HashMap<>();
        sc.textFile(inPath + "/ProductDisplayInfo.csv")
                .map(parser::mapProductDisplayInfo)
                .collect()
                .forEach(info -> prodDsplInfoMap.put(info.getProdCatgCd(), info));

        // 고객 상품 별 임시(ContentVisit)
        JavaRDD<ContentVisit> contentVisitRDD =
                baseDetailRDD
                        .groupBy(base -> base.getSessionId())
                        .mapValues(base -> parser.mapContentVisit(base))
                        .values()
                        .flatMap(iter -> iter.iterator());

        //Temp
        //sqlContext.createDataFrame(contentVisitRDD, ContentVisit.class)
        //        .write().mode(SaveMode.Overwrite).parquet(outPath + "/ContentVisit" + "/dt=" + date);
        //Temp

        // 컨텐츠별 방문요약(일별)(ContentVisitSummary)
        JavaRDD<ContentVisitSummary> contentVisitSummaryRDD =
                contentVisitRDD
                        .mapToPair(content -> new Tuple2<>(
                                buildKey(content.getProductCd(), content.getCategoryCdL()
                                        , content.getCategoryCdM(), content.getCategoryCdS()
                                        , content.getCurrPageUri())
                                , content)
                        )
                        .aggregateByKey(new Tuple2<>(new ContentVisit()
                                        , new Tuple2<>(new HashSet<>(), new HashSet<>()))
                                , parser::seqSumCustVisit
                                , parser::combineSumCustVisit
                        )
                        .values()
                        .map(t -> parser.mapContentVisitSummary(date, prodInfoMap, prodDsplInfoMap, t));
        sqlContext.createDataFrame(contentVisitSummaryRDD, ContentVisitSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/ContentVisitSummary" + "/dt=" + date);

        // 고객별 상품별 방문집계(일별)(CustProductDaily)
        JavaRDD<CustProductDaily> custProductDailyRDD =
                contentVisitRDD
                        .mapToPair(content -> new Tuple2<>(
                                        buildKey(content.getCustId(), content.getCookieId(), content.getProductCd()
                                                , content.getCategoryCdL(), content.getCategoryCdM(), content.getCategoryCdS())
                                        , content
                                )
                        )
                        .reduceByKey(parser::sumCustVisit)
                        .values()
                        .map(content -> parser.mapCustProductDaily(date, prodInfoMap, prodDsplInfoMap, content));
        sqlContext.createDataFrame(custProductDailyRDD, CustProductDaily.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/CustProductDaily" + "/dt=" + date);

        // 상품별 방문요약(일별)(ProductVisitSummary)
        JavaRDD<ProductVisitSummary> productVisitSummaryDailyRDD =
                custProductDailyRDD
                        .mapToPair(product ->
                                new Tuple2<>(buildKey(product.getProduct_cd(), product.getProduct_catg_l()
                                        , product.getProduct_catg_m(), product.getProduct_catg_s())
                                        , parser.mapProductVisitSummary(product))
                        )
                        .reduceByKey(parser::sumProductVisitSummary)
                        .values();
        sqlContext.createDataFrame(productVisitSummaryDailyRDD, ProductVisitSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/ProductVisitSummary" + "/dt=" + date);

        // 시간대 별 방문요약(HourVisitSummary)
        JavaRDD<HourVisitSummary> hourVisitSummaryRDD =
                baseDetailRDD
                        .groupBy(base -> base.getSessionId())
                        .mapValues(base -> parser.statHourVisitSummary(date, base))
                        .values()
                        .flatMap(iter -> iter.iterator())
                        .mapToPair(summ -> new Tuple2<>(summ.getBase_hr(), summ))
                        .reduceByKey((acc, curr) -> {
                            acc.addVisit_cnt(curr.getVisit_cnt());
                            acc.addPageview_cnt(curr.getPageview_cnt());
                            return acc;
                        })
                        .values();
        sqlContext.createDataFrame(hourVisitSummaryRDD, HourVisitSummary.class)
                .write().mode(SaveMode.Overwrite).parquet(outPath + "/HourVisitSummary" + "/dt=" + date);

    }

    public void setUpSparkConfig(boolean isLocal) {
        sparkConf = new SparkConf().setAppName("Daishin Realtime Data Batch");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class<?>[]{LogBaseDetail.class, OrgOrderLog.class});
    }

    private String buildKey(String... keys) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) sb.append(":");
            sb.append(convEmptyValue(keys[i]));
        }

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

    private static String removeQuotes(String data) {
        if (data.startsWith("\""))
            data = data.substring(1);
        if (data.endsWith("\""))
            data = data.substring(0, data.lastIndexOf("\""));
        return data;
    }
}