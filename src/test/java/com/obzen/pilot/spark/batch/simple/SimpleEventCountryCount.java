package com.obzen.pilot.spark.batch.simple;

import com.obzen.pilot.spark.common.data.EventInfo;
import com.obzen.pilot.spark.common.util.CommonUtil;
import com.obzen.pilot.spark.common.util.JsonParserUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * Created by hanmin on 16. 2. 1.
 */
public class SimpleEventCountryCount {

    public static void main(String[] args) {

        SimpleEventCountryCount count = new SimpleEventCountryCount();
        count.runEvent("/home/hanmin/Documents/meetup/20160211/OpenEvents.log");

    }

    /**
     * java-8 lambda expression
     * @param filename
     */
    public void runEvent(String filename) {
        SparkConf sparkConf = new SparkConf().setAppName("WordCountDemo").setMaster("local[2]");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        long milliTime = System.currentTimeMillis();
        JavaRDD<String> lines = ctx.textFile(filename, 1).filter(line -> line.contains("utc_offset"));

        //JavaRDD<EventInfo> mapRdd = lines.map(line -> new EventInfo(line));
        JavaRDD<EventInfo> mapRdd = lines.map(line -> new JsonParserUtil().parserEventInfo(line));

        //-----------------------------------------------------------------
        // cache 영역 저장
        mapRdd.cache();


        //=================================================================
        //=================================================================

        sumByCountry(mapRdd, milliTime);

        sumByCity(mapRdd, milliTime);

        //=================================================================
        //=================================================================



        //-----------------------------------------------------------------
        // cache 영역제거
        mapRdd.unpersist();

        ctx.close();
    }

    public void sumByCity(JavaRDD<EventInfo> mapRdd, long milliTime){
        JavaPairRDD<String, Integer> mapToPairRdd = mapRdd.mapToPair(
                record -> new Tuple2<String, Integer>(CommonUtil.toUpperCase(record.getVenueInfo().getCountry())+" | "+record.getVenueInfo().getCity(), 1) );

        JavaPairRDD<String, Integer> reduceByKeyRdd = mapToPairRdd.reduceByKey( (v1, v2) -> v1+v2); // (za | Pretoria,1)

        JavaPairRDD<Integer, String> swappedPairRdd = reduceByKeyRdd.mapToPair(item -> item.swap()); // (28,ca | Vancouver)

        swappedPairRdd.sortByKey(false).saveAsTextFile("/working/testdata/countByCity"+ milliTime);
    }

    public void sumByCountry(JavaRDD<EventInfo> mapRdd, long milliTime){
        JavaPairRDD<String, Integer> mapToPairRdd = mapRdd.mapToPair(
                record -> new Tuple2<String, Integer>(CommonUtil.toUpperCase(record.getVenueInfo().getCountry()), 1) );

        JavaPairRDD<String, Integer> reduceByKeyRdd = mapToPairRdd.reduceByKey( (v1, v2) -> v1+v2); // (za | Pretoria,1)

        JavaPairRDD<Integer, String> swappedPairRdd = reduceByKeyRdd.mapToPair(item -> item.swap()); // (28,ca | Vancouver)

        swappedPairRdd.sortByKey(false).saveAsTextFile("/working/testdata/countByCountry"+ milliTime);
    }

    /**
     *
     * @param lines
     */
    public void java7expression(final JavaRDD<String> lines) {

        JavaRDD<EventInfo> mapRdd = lines.map(
                new Function<String, EventInfo>() {
                    public EventInfo call(String line) throws Exception {
                        EventInfo sd = new JsonParserUtil().parserEventInfo(line);
                        return sd;
                    }
                });

        // cache 영역 저장 (https://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence)
        //mapRdd.persist(StorageLevel.MEMORY_ONLY()); // StorageLevel.MEMORY_ONLY_SER() , object not serializable org.json.simple.parser.JSONParser
        mapRdd.cache();

        JavaPairRDD<String, Integer> mapToPairRdd =
                mapRdd.mapToPair(new PairFunction<EventInfo, String, Integer>() {
                    public Tuple2<String, Integer> call(EventInfo record) {
                        return new Tuple2(record.getVenueInfo().getCountry()+" | "+record.getVenueInfo().getCity(), 1);
                        //return t2;
                    }
                });

        JavaPairRDD<String, Integer> reduceByKeyRdd =
                mapToPairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                }); // (za | Pretoria,1)

        JavaPairRDD<Integer, String> swappedPairRdd = reduceByKeyRdd.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                return item.swap();
            }

        }); // (28,ca | Vancouver)

        /**
         * sortByKey(([ascending], [numTasks]) , numTasks : part-00000의 갯수
         */
        swappedPairRdd.sortByKey(false).saveAsTextFile("/working/testdata/country_count"+ System.currentTimeMillis());
    }

}
