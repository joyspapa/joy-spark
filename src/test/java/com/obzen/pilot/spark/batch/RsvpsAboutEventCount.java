package com.obzen.pilot.spark.batch;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obzen.pilot.spark.common.data.EventInfo;
import com.obzen.pilot.spark.common.data.RsvpsInfo;
import com.obzen.pilot.spark.common.util.CommonUtil;
import com.obzen.pilot.spark.common.util.JsonParserUtil;

import scala.Tuple2;
import scala.Tuple4;

/**
 * ※ ☆ ★ ○ ● ◎ ◇ ◆ □ ■ △ ▲ ▽ ▼ → ← ↑ ↓ ↔ 〓 ◁ ◀ ▷ ▶ ♤ ♠ ♡ ♥ ♧ ♣ ⊙◈ ▣ ◐ ◑
 * Created by hanmin on 16. 2. 1.
 */
public class RsvpsAboutEventCount {

	final Logger logger = LoggerFactory.getLogger(RsvpsAboutEventCount.class);

	public static void main(String[] args) {

		RsvpsAboutEventCount count = new RsvpsAboutEventCount();
		count.runEvent("/home/hanmin/Documents/meetup/20160211/OpenEvents.log");

	}

	public void runEvent(String filename) {
		SparkConf sparkConf = new SparkConf().setAppName("WordCountDemo2").setMaster("local[3]");
		sparkConf.set("spark.scheduler.mode", "FAIR");

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		long milliTime = System.currentTimeMillis();

		//scenario1(ctx, milliTime );

		scenario2(ctx, milliTime);

		//scenario3(ctx, milliTime );

		//        JavaRDD<String> eventLines = ctx.textFile("/home/hanmin/Documents/meetup/20160211/OpenEvents.log", 1).filter(line -> line.contains("utc_offset"));
		//        JavaRDD<EventInfo> eventMapRdd = eventLines.map(line -> new JsonParserUtil().parserEventInfo(line));
		//        JavaPairRDD<String, Tuple4<String, String, String, String>> eventMapToPairRdd = eventMapRdd.mapToPair(
		//                record -> new Tuple2<String, Tuple4<String, String, String, String>>(record.getEventId(),
		//                        new Tuple4(record.getEventName(), record.getGroupSub().getCategory().getShortname(), record.getVenueInfo().getCountry(), record.getVenueInfo().getCity() ))  // (lqxzplyvfbdb,1)
		//        ); // 228788060,(("Domingay",socializing,es,Madrid)
		//
		//
		//
		//        JavaRDD<String> rsvpsLines = ctx.textFile("/home/hanmin/Documents/meetup/20160211/Rsvps.log", 1).filter(line -> line.contains("response"));
		//        JavaRDD<RsvpsInfo> rsvpsMapRdd = rsvpsLines.map(line -> new JsonParserUtil().parserRsvpsInfo(line));
		//
		//        rsvpsMapRdd.cache();
		//
		//        JavaPairRDD<String, Integer> rsvpsMapToPairRdd2 = rsvpsMapRdd.mapToPair(
		//                record -> new Tuple2<String, Integer>(
		//                        record.getEvent().getEventId()+"_"+record.getResponse(), 1)   // (228735191,(yes,1))
		//                );
		//        JavaPairRDD<String, Integer> rsvpsReduceByKey2 =
		//                rsvpsMapToPairRdd2.reduceByKey((i1,i2) -> (i1+i2)  // (228735191,(yes,4))
		//                );
		//
		//        JavaPairRDD<String, Tuple2<String, Integer>> rsvpsMapToPairRdd3 = rsvpsReduceByKey2.mapToPair(
		//                item->new Tuple2<String, Tuple2<String, Integer>>(
		//                        item._1.split("_")[0],
		//                        new Tuple2<String, Integer>(
		//                                item._1.split("_")[1],
		//                                item._2
		//                                )
		//                )
		//
		//        );
		//
		//
		//        JavaPairRDD<String, Tuple2<Tuple4<String, String, String, String>, Tuple2<String, Integer>>> result = eventMapToPairRdd.join(rsvpsMapToPairRdd3);
		//        result.cache();

		//        result.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Tuple2<String, Integer>>>>() {
		//                            public void call(Tuple2<String, Tuple2<String, Tuple2<String, Integer>>> line) {
		//
		//                            }
		//                }
		//        );

		//        List<Tuple2<String, Tuple2<String, Tuple2<String, Integer>>>> output = result.collect();
		//        for (Tuple2<?, ?> tuple : output) {
		//            //logger.info(tuple._1() + ": " + tuple._2());
		//        }

		//        result.saveAsTextFile("/working/testdata/RsvpsAboutEventCount_"+ milliTime);
		//        result.unpersist();
		//
		//        rsvpsMapRdd.unpersist();
		//
		//        ctx.close();
	}

	/**
	 * 응답이 제일 많은 이벤트 추출
	 * (49,(228800309,("FinTech Berlin February Meetup",career-business,de,Berlin)))
	   (31,(228799252,("Saturday morning football at Astro Arena",sports-recreation,IN,Bangalore)))
	   (24,(228784442,("Machine Learning with Python",tech,fi,Helsinki)))
	   (21,(228783338,("Mapas y Tooltips con D3.js",tech,es,Madrid)))
	 */
	public void scenario1(JavaSparkContext ctx, long startTime) {
		JavaRDD<String> eventLines = ctx.textFile("/home/hanmin/Documents/meetup/20160211/OpenEvents.log", 1)
				.filter(line -> line.contains("utc_offset"));
		JavaRDD<EventInfo> eventMapRdd = eventLines.map(line -> new JsonParserUtil().parserEventInfo(line));
		JavaPairRDD<String, Tuple4<String, String, String, String>> eventMapToPairRdd = eventMapRdd
				.mapToPair(record -> new Tuple2<String, Tuple4<String, String, String, String>>(record.getEventId(),
						new Tuple4(record.getEventName(), record.getGroupSub().getCategory().getShortname(),
								record.getVenueInfo().getCountry(), record.getVenueInfo().getCity())) // (lqxzplyvfbdb,1)
		); // 228788060,(("Domingay",socializing,es,Madrid)

		JavaRDD<String> rsvpsLines = ctx.textFile("/home/hanmin/Documents/meetup/20160211/Rsvps.log", 1)
				.filter(line -> line.contains("response"));
		JavaRDD<RsvpsInfo> rsvpsMapRdd = rsvpsLines.map(line -> new JsonParserUtil().parserRsvpsInfo(line));
		JavaPairRDD<String, Integer> rsvpsMapToPairRdd2 = rsvpsMapRdd
				.mapToPair(record -> new Tuple2<String, Integer>(record.getEvent().getEventId(), 1) // (228735191,(yes,1))
		);
		JavaPairRDD<String, Integer> rsvpsReduceByKey2 = rsvpsMapToPairRdd2.reduceByKey((i1, i2) -> (i1 + i2) // (228735191,(yes,4))
		);

		JavaPairRDD<String, Tuple2<Tuple4<String, String, String, String>, Integer>> result = eventMapToPairRdd
				.join(rsvpsReduceByKey2);
		//result.saveAsTextFile("/working/testdata/RsvpsAboutEventCount(scenario1)_"+ startTime); // (228788060,(("Domingay",socializing,es,Madrid),6))

		JavaPairRDD<Integer, Tuple2<String, Tuple4<String, String, String, String>>> swappedPairRdd = result.mapToPair(
				item -> new Tuple2<Integer, Tuple2<String, Tuple4<String, String, String, String>>>(item._2._2,
						new Tuple2(item._1, item._2._1)));

		//        JavaPairRDD<Integer, Tuple2<String, Tuple4<String, String, String, String>>> swappedPairRdd = result.mapToPair(
		//                new PairFunction<Tuple2<String, Tuple2<Tuple4<String, String, String, String>, Integer>>, Integer, Tuple2<String, Tuple4<String, String, String, String>>>() {
		//            public Tuple2<Integer, Tuple2<String, Tuple4<String, String, String, String>>> call(Tuple2<String, Tuple2<Tuple4<String, String, String, String>, Integer>> item) {
		//                return new Tuple2<Integer, Tuple2<String, Tuple4<String, String, String, String>>>(
		//                        item._2._2,
		//                        new Tuple2(item._1, item._2._1)
		//                );
		//            }
		//        });

		swappedPairRdd.cache();

		//swappedPairRdd.sortByKey(false).saveAsTextFile("/working/testdata/RsvpsAboutEventCount(scenario1)_"+ startTime); // (49,(228800309,("FinTech Berlin February Meetup",career-business,de,Berlin)))

		List<Tuple2<Integer, Tuple2<String, Tuple4<String, String, String, String>>>> output = swappedPairRdd
				.sortByKey(false).collect();
		int limitCnt = 0;
		logger.info(beutifulize("응답 수", 8) + " " + beutifulize("이벤트 명", 76) + " " + beutifulize("카테고리", 27) + " "
				+ beutifulize("도시", 10)); // 49: 228800309 ~ 12: 228799223
		logger.info(beutifulize("-----", 8) + " " + beutifulize("---------------", 80) + " "
				+ beutifulize("--------", 30) + " " + beutifulize("--------", 15)); // 49: 228800309 ~ 12: 228799223
		for (Tuple2<Integer, Tuple2<String, Tuple4<String, String, String, String>>> tuple : output) {
			if (limitCnt > 11)
				break;
			logger.info(beutifulize(String.valueOf(tuple._1()), 8) + " " + beutifulize(tuple._2._2._1(), 80)
					+ " " + beutifulize(tuple._2._2._2(), 30) + " " + beutifulize(tuple._2._2._4(), 15)); // 49: 228800309 ~ 12: 228799223
			limitCnt++;
		}

		swappedPairRdd.unpersist();
		ctx.close();
	}

	/**
	 *  참석(yes)응답이 제일 많은 이벤트 추출
	 * (48,(228800309,("FinTech Berlin February Meetup",career-business,de,Berlin)))
	 (28,(228799252,("Saturday morning football at Astro Arena",sports-recreation,IN,Bangalore)))
	 (24,(228784442,("Machine Learning with Python",tech,fi,Helsinki)))
	 (21,(228783338,("Mapas y Tooltips con D3.js",tech,es,Madrid)))
	 */
	public void scenario2(JavaSparkContext ctx, long startTime) {
		JavaRDD<String> eventLines = ctx.textFile("/home/hanmin/Documents/meetup/20160211/OpenEvents.log", 1)
				.filter(line -> line.contains("utc_offset"));
		JavaRDD<EventInfo> eventMapRdd = eventLines.map(line -> new JsonParserUtil().parserEventInfo(line));
		JavaPairRDD<String, Tuple4<String, String, String, String>> eventMapToPairRdd = eventMapRdd
				.mapToPair(record -> new Tuple2<String, Tuple4<String, String, String, String>>(record.getEventId(),
						new Tuple4(record.getEventName(), record.getGroupSub().getCategory().getShortname(),
								record.getVenueInfo().getCountry(), record.getVenueInfo().getCity())) // (lqxzplyvfbdb,1)
		); // 228788060,(("Domingay",socializing,es,Madrid)

		JavaRDD<String> rsvpsLines = ctx.textFile("/home/hanmin/Documents/meetup/20160211/Rsvps.log", 1)
				.filter(line -> line.contains("\"response\":\"yes\""));
		JavaRDD<RsvpsInfo> rsvpsMapRdd = rsvpsLines.map(line -> new JsonParserUtil().parserRsvpsInfo(line));
		JavaPairRDD<String, Integer> rsvpsMapToPairRdd2 = rsvpsMapRdd
				.mapToPair(record -> new Tuple2<String, Integer>(record.getEvent().getEventId(), 1) // (228735191,(yes,1))
		);
		JavaPairRDD<String, Integer> rsvpsReduceByKey2 = rsvpsMapToPairRdd2.reduceByKey((i1, i2) -> (i1 + i2) // (228735191,4))
		);

		//        JavaPairRDD<String, Tuple2<String, Integer>> rsvpsMapToPairRdd3 = rsvpsReduceByKey2.mapToPair(
		//                item->new Tuple2<String, Tuple2<String, Integer>>(
		//                        item._1.split("_")[0],
		//                        new Tuple2<String, Integer>(
		//                                item._1.split("_")[1],
		//                                item._2
		//                                )
		//                )
		//
		//        );

		JavaPairRDD<String, Tuple2<Tuple4<String, String, String, String>, Integer>> result = eventMapToPairRdd
				.join(rsvpsReduceByKey2);
		//result.saveAsTextFile("/working/testdata/RsvpsAboutEventCount(scenario2)_"+ startTime); // (228788060,(("Domingay",socializing,es,Madrid),6))

		JavaPairRDD<Integer, Tuple2<String, Tuple4<String, String, String, String>>> swappedPairRdd = result.mapToPair(
				item -> new Tuple2<Integer, Tuple2<String, Tuple4<String, String, String, String>>>(item._2._2,
						new Tuple2(item._1, item._2._1)));

		//        JavaPairRDD<Integer, Tuple2<String, Tuple4<String, String, String, String>>> swappedPairRdd = result.mapToPair(
		//                new PairFunction<Tuple2<String, Tuple2<Tuple4<String, String, String, String>, Integer>>, Integer, Tuple2<String, Tuple4<String, String, String, String>>>() {
		//            public Tuple2<Integer, Tuple2<String, Tuple4<String, String, String, String>>> call(Tuple2<String, Tuple2<Tuple4<String, String, String, String>, Integer>> item) {
		//                return new Tuple2<Integer, Tuple2<String, Tuple4<String, String, String, String>>>(
		//                        item._2._2,
		//                        new Tuple2(item._1, item._2._1)
		//                );
		//            }
		//        });

		swappedPairRdd.cache();

		//swappedPairRdd.sortByKey(false).saveAsTextFile("/working/testdata/RsvpsAboutEventCount(scenario2)_"+ startTime); // (48,(228800309,("FinTech Berlin February Meetup",career-business,de,Berlin)))

		List<Tuple2<Integer, Tuple2<String, Tuple4<String, String, String, String>>>> output = swappedPairRdd
				.sortByKey(false).collect();
		int limitCnt = 0;
		logger.info(beutifulize("참석 응답 수", 15) + " " + beutifulize("이벤트 명", 76) + " " + beutifulize("카테고리", 27)
				+ " " + beutifulize("도시", 10)); // 49: 228800309 ~ 12: 228799223
		logger.info(beutifulize("-----", 15) + " " + beutifulize("---------------", 80) + " "
				+ beutifulize("--------", 30) + " " + beutifulize("--------", 15)); // 49: 228800309 ~ 12: 228799223
		for (Tuple2<Integer, Tuple2<String, Tuple4<String, String, String, String>>> tuple : output) {
			if (limitCnt > 11)
				break;
			logger.info(beutifulize(String.valueOf(tuple._1()), 8) + " " + beutifulize(tuple._2._2._1(), 80)
					+ " " + beutifulize(tuple._2._2._2(), 30) + " " + beutifulize(tuple._2._2._4(), 15)); // 49: 228800309 ~ 12: 228799223
			limitCnt++;
		}

		swappedPairRdd.unpersist();
		ctx.close();
	}

	/**
	 *  응답이 제일 많은 이벤트 발생 도시 별 추출
	 *  (172,Sydney[AU])
	    (172,Singapore[SG])
	    (134,Vancouver[CA])
	    (120,Portland[US])
	 *
	 */
	public void scenario3(JavaSparkContext ctx, long startTime) {
		JavaRDD<String> eventLines = ctx.textFile("/home/hanmin/Documents/meetup/20160211/OpenEvents.log", 1)
				.filter(line -> line.contains("utc_offset"));
		JavaRDD<EventInfo> eventMapRdd = eventLines.map(line -> new JsonParserUtil().parserEventInfo(line));
		JavaPairRDD<String, Tuple2<String, String>> eventMapToPairRdd = eventMapRdd.mapToPair(
				record -> new Tuple2<String, Tuple2<String, String>>(record.getEventId(), new Tuple2<String, String>(
						CommonUtil.toUpperCase(record.getVenueInfo().getCountry()), record.getVenueInfo().getCity())) // (lqxzplyvfbdb,1)
		); // 228788060,(("Domingay",socializing,es,Madrid)

		JavaRDD<String> rsvpsLines = ctx.textFile("/home/hanmin/Documents/meetup/20160211/Rsvps.log", 1)
				.filter(line -> line.contains("\"response\""));
		JavaRDD<RsvpsInfo> rsvpsMapRdd = rsvpsLines.map(line -> new JsonParserUtil().parserRsvpsInfo(line));
		JavaPairRDD<String, Integer> rsvpsMapToPairRdd2 = rsvpsMapRdd
				.mapToPair(record -> new Tuple2<String, Integer>(record.getEvent().getEventId(), 1) // (228735191,(yes,1))
		);

		JavaPairRDD<String, Tuple2<Tuple2<String, String>, Integer>> result = eventMapToPairRdd
				.join(rsvpsMapToPairRdd2);
		//result.saveAsTextFile("/working/testdata/RsvpsAboutEventCount(scenario3)_"+ startTime); // (228788060,((es,Madrid),1))

		JavaPairRDD<String, Integer> mapToPairRdd = result
				.mapToPair(item -> new Tuple2<String, Integer>(item._2._1._2 + "[" + item._2._1._1 + "]", 1) // (Madrid(es), 1)
		);

		JavaPairRDD<String, Integer> reduceByKeyRdd = mapToPairRdd.reduceByKey((i1, i2) -> i1 + i2);

		//        JavaPairRDD<String, Integer> reduceByKeyRdd = mapToPairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
		//            @Override
		//            public Integer call(Integer i1, Integer i2) throws Exception {
		//                return i1+i2;
		//            }
		//        });

		JavaPairRDD<Integer, String> swappedPairRdd = reduceByKeyRdd.mapToPair(item -> item.swap());
		swappedPairRdd.cache();

		//swappedPairRdd.sortByKey(false).saveAsTextFile("/working/testdata/RsvpsAboutEventCount(scenario3)_"+ startTime); // (48,(228800309,("FinTech Berlin February Meetup",career-business,de,Berlin)))

		List<Tuple2<Integer, String>> output = swappedPairRdd.sortByKey(false).collect();
		int limitCnt = 0;
		logger.info(beutifulize("응답 수", 8) + " " + beutifulize("도시명[국가]", 30)); // 49: 228800309 ~ 12: 228799223
		logger.info(beutifulize("-----", 8) + " " + beutifulize("---------------", 30)); // 49: 228800309 ~ 12: 228799223
		for (Tuple2<Integer, String> tuple : output) {
			if (limitCnt > 50)
				break;
			logger.info(beutifulize(String.valueOf(tuple._1()), 8) + " " + beutifulize((tuple._2), 30)); // 49: 228800309 ~ 12: 228799223
			limitCnt++;
		}

		swappedPairRdd.unpersist();
		ctx.close();
	}

	/**
	 * (228791884,(friday night 18th March, Peppermint Jam, Penrith RSL,(no,2)))
	 * (228791884,(friday night 18th March, Peppermint Jam, Penrith RSL,(yes,1)))
	 * (228791884,(Dinner & Dancing to Peppermint Jam, Penrith RSL,(no,2)))
	 * (228791884,(Dinner & Dancing to Peppermint Jam, Penrith RSL,(yes,1)))
	 *
	 * @param filename
	 */
	public void runEvent_20160212_1742(String filename) {
		SparkConf sparkConf = new SparkConf().setAppName("WordCountDemo").setMaster("local[2]");

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		long milliTime = System.currentTimeMillis();

		//============================================================
		//====================== OpenEvent ===========================
		// (event_id , event_name)
		JavaRDD<String> eventLines = ctx.textFile(filename, 1).filter(line -> line.contains("utc_offset"));
		JavaRDD<EventInfo> eventMapRdd = eventLines.map(line -> new JsonParserUtil().parserEventInfo(line));
		JavaPairRDD<String, String> eventMapToPairRdd = eventMapRdd
				.mapToPair(record -> new Tuple2<String, String>(record.getEventId(), record.getEventName()));

		//============================================================
		// ====================== Rsvps ================================
		JavaRDD<String> rsvpsLines = ctx.textFile("/home/hanmin/Documents/meetup/20160211/Rsvps.log", 1)
				.filter(line -> line.contains("response"));
		JavaRDD<RsvpsInfo> rsvpsMapRdd = rsvpsLines.map(line -> new JsonParserUtil().parserRsvpsInfo(line));

		rsvpsMapRdd.cache();

		// (eventId_eventRsvp, 1)  -> (eventId_eventRsvp , sum )
		JavaPairRDD<String, Integer> rsvpsMapToPairRdd2 = rsvpsMapRdd.mapToPair(
				record -> new Tuple2<String, Integer>(record.getEvent().getEventId() + "_" + record.getResponse(), 1) // (228735191,(yes,1))
		);
		JavaPairRDD<String, Integer> rsvpsReduceByKey2 = rsvpsMapToPairRdd2.reduceByKey((i1, i2) -> (i1 + i2) // (228735191,(yes,4))
		);

		// (eventId_eventRsvp , sum ) -> (eventId , (eventRsvp , sum ))
		JavaPairRDD<String, Tuple2<String, Integer>> rsvpsMapToPairRdd3 = rsvpsReduceByKey2
				.mapToPair(item -> new Tuple2<String, Tuple2<String, Integer>>(item._1.split("_")[0],
						new Tuple2<String, Integer>(item._1.split("_")[1], item._2)));
						//rsvpsMapToPairRdd3.sortByKey(false).saveAsTextFile("/working/testdata/rsvpsReduceByKey3_"+ milliTime);

		// (228791884,(Dinner & Dancing to Peppermint Jam, Penrith RSL,(yes,1)))
		JavaPairRDD<String, Tuple2<String, Tuple2<String, Integer>>> result = eventMapToPairRdd
				.join(rsvpsMapToPairRdd3);

		result.saveAsTextFile("/working/testdata/RsvpsAboutEventCount_" + milliTime);

		rsvpsMapRdd.unpersist();
		ctx.close();
	}

	/**
	 * result example :
	 * (228773868,(Hackster Arduino Hands On,(yes,2)))
	 * (224245354,(Destination: JAMAICA,(no,2)))
	 *
	 * @param filename
	 */
	public void runEvent_20160212_1527(String filename) {
		SparkConf sparkConf = new SparkConf().setAppName("WordCountDemo").setMaster("local[2]");

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		long milliTime = System.currentTimeMillis();

		JavaRDD<String> eventLines = ctx.textFile(filename, 1).filter(line -> line.contains("utc_offset"));
		JavaRDD<EventInfo> eventMapRdd = eventLines.map(line -> new JsonParserUtil().parserEventInfo(line));
		JavaPairRDD<String, String> eventMapToPairRdd = eventMapRdd
				.mapToPair(record -> new Tuple2<String, String>(record.getEventId(), record.getEventName()) // (lqxzplyvfbdb,1)
		);

		eventMapToPairRdd.sortByKey(false).saveAsTextFile("/working/testdata/eventMapToPairRdd_" + milliTime);

		JavaRDD<String> rsvpsLines = ctx.textFile("/home/hanmin/Documents/meetup/20160211/Rsvps.log", 1)
				.filter(line -> line.contains("response"));
		JavaRDD<RsvpsInfo> rsvpsMapRdd = rsvpsLines.map(line -> new JsonParserUtil().parserRsvpsInfo(line));
		JavaPairRDD<String, Tuple2<String, Integer>> rsvpsMapToPairRdd = rsvpsMapRdd
				.mapToPair(record -> new Tuple2<String, Tuple2<String, Integer>>(record.getEvent().getEventId(),
						new Tuple2<String, Integer>(record.getResponse(), 1) // (228394574,(yes,1))
		));

		JavaPairRDD<String, Tuple2<String, Integer>> rsvpsReduceByKey = rsvpsMapToPairRdd
				.reduceByKey((v1, v2) -> new Tuple2<String, Integer>(v1._1, v1._2 + v2._2) // (228394574,(yes,1))
		);

		rsvpsReduceByKey.saveAsTextFile("/working/testdata/rsvpsMapToPairRdd_" + milliTime);

		JavaPairRDD<String, Tuple2<String, Tuple2<String, Integer>>> result = eventMapToPairRdd.join(rsvpsReduceByKey);

		result.saveAsTextFile("/working/testdata/RsvpsAboutEventCount_" + milliTime);

		ctx.close();
	}

	public void java7version(JavaPairRDD<String, Tuple2<String, Integer>> rsvpsMapToPairRdd) {
		// 아래 로직이 있으면 Serialize Error가 발생
		JavaPairRDD<String, Tuple2<String, Integer>> rsvpsReduceByKey = rsvpsMapToPairRdd.reduceByKey(
				new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					public Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2)
							throws Exception {
						return new Tuple2<String, Integer>(v1._1, v1._2 + v2._2); // (228394574,(yes,1))
					}
				});

		//        JavaPairRDD<String, Tuple2<String, Integer>> rsvpsMapToPairRdd3 = rsvpsReduceByKey2.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>() {
		//            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<String, Integer> item) throws Exception {
		//                return new Tuple2<String, Tuple2<String, Integer>>(
		//                        item._1.split("_")[0],
		//                        new Tuple2<String, Integer>(
		//                                item._1.split("_")[1],
		//                                item._2
		//                        )
		//                );
		//            }
		//
		//        });
	}

	public String beutifulize(String input, int num) {
		StringBuilder sb = new StringBuilder();
		sb.append(input);
		for (int i = 0; i < num; i++) {
			sb.append(" ");
		}
		return sb.toString().substring(0, num);
	}
}
