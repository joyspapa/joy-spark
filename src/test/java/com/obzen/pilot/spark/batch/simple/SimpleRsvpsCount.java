package com.obzen.pilot.spark.batch.simple;

import com.obzen.pilot.spark.common.data.RsvpsInfo;
import com.obzen.pilot.spark.common.util.JsonParserUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by hanmin on 16. 2. 11.
 */
public class SimpleRsvpsCount {

    public static void main(String[] args) {

        SimpleRsvpsCount count = new SimpleRsvpsCount();
        count.runEvent("/home/hanmin/Documents/meetup/20160211/Rsvps.log");

    }

    /**
     * java-8 lambda expression
     * @param filename
     */
    public void runEvent(String filename) {
        SparkConf sparkConf;
        JavaSparkContext ctx = null;
        try {
            sparkConf = new SparkConf().setAppName("RsvpsCountDemo").setMaster("local[2]");
            //sparkConf.set("spark.scheduler.mode", "FAIR");

            ctx = new JavaSparkContext(sparkConf);

            long milliTime = System.currentTimeMillis();
            JavaRDD<String> lines = ctx.textFile(filename, 1).filter(line -> line.contains("response"));

            //JavaRDD<EventInfo> mapRdd = lines.map(line -> new EventInfo(line));
            JavaRDD<RsvpsInfo> mapRdd = lines.map(line -> new JsonParserUtil().parserRsvpsInfo(line));

            /*------------------------------
            cache 영역 저장
            ------------------------------*/
            mapRdd.cache();


            // ==============================
            // User Code 영역
            // ==============================

            sumByResponse(mapRdd, milliTime);


            /*------------------------------
            cache 영역제거
            ------------------------------*/
            mapRdd.unpersist();
        } finally {
            if(ctx != null) {
                ctx.close();
                ctx = null;
            }
        }
    }

    public void sumByResponse(JavaRDD<RsvpsInfo> mapRdd, long milliTime){
        JavaPairRDD<String, Integer> mapToPairRdd = mapRdd.mapToPair(
                record -> new Tuple2<String, Integer>(record.getEvent().getEventId(), 1) );

        JavaPairRDD<String, Integer> reduceByKeyRdd = mapToPairRdd.reduceByKey( (v1, v2) -> v1+v2); // (za | Pretoria,1)

        JavaPairRDD<Integer, String> swappedPairRdd = reduceByKeyRdd.mapToPair(item -> item.swap()); // (28,ca | Vancouver)

        swappedPairRdd.sortByKey(false).saveAsTextFile("/working/testdata/countByResponse"+ milliTime);
    }

}
