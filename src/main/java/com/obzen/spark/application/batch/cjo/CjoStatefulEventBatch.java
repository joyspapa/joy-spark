package com.obzen.spark.application.batch.cjo;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obzen.spark.application.batch.cjo.parser.CjoBaseLogParser;
import com.obzen.spark.application.batch.cjo.parser.CjoStatefulScenarioAnalysis;
import com.obzen.spark.application.batch.cjo.pojo.CJO_LOG_STATEFUL;
import com.obzen.spark.application.batch.cjo.pojo.CJO_LOG_STATEFUL_SUM;

import scala.Tuple2;

/**
 * Created by hanmin on 17. 3. 2.
 */
public class CjoStatefulEventBatch implements Serializable {
	private static final long serialVersionUID = 8965924708608823997L;
	private static final Logger logger = LoggerFactory.getLogger(CjoStatefulEventBatch.class);
	private static String baseLogHdfsRootDir = "/user/ecube/obzen/parquet/obzen-app-MAIN_BASE-ENT9336-HDFS/CJO_LOG_BASE/date_yyyymmdd=";
	private static String sumHdfsRootDir = "/user/ecube/obzen/parquet/stateful/";
	
	//private static SparkSession sparkSession;
	private SparkConf sparkConf;
	private SQLContext sqlContext;
	//private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
	CjoBaseLogParser parser = new CjoBaseLogParser();

	public void setUpSparkConfig(boolean isLocal) {
		sparkConf = new SparkConf().setAppName("CJ OShopping Spark Batch Application");
		//sparkConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		//sc.set("spark.kryo.registrationRequired", "true");
		//sparkConf.registerKryoClasses(new Class<?>[] { KafkaProducerWrapperCJO.class/*, CJmallLogVO.class*/ });

		sparkConf.setMaster("local[*]");

		//		Builder sparkBuilder = SparkSession.builder().appName("FileToKafkaBatchTest")
		//				.config("parquet.enable.summary-metadata", false).config("parquet.metadata.read.parallelism", "10")
		//				.config("mapreduce.fileoutputcommitter.marksuccessfuljobs", false).config(sparkConf);
		//		if (isLocal) {
		//			sparkBuilder.master("local[*]");
		//		}
		//		sparkSession = sparkBuilder.getOrCreate();
	}

	public static void main(String[] args) throws Exception {

		logger.info("[main] begin ...");
		long startTime = System.currentTimeMillis();

		CjoStatefulEventBatch simple = new CjoStatefulEventBatch();
		
		String hdfsUrl = "";
		String date = "";
		String scenario = "sc03";
		boolean isLocal = false;
		if(args == null || args.length < 2) {
			isLocal = true;
			date = "2020-01-23";
			hdfsUrl = "hdfs://192.168.10.88:8020";
		} else {
			date = args[0];
			scenario = args[1];
		}
		
		String sourceFileRootDir = hdfsUrl + baseLogHdfsRootDir + date;
		sumHdfsRootDir = hdfsUrl + sumHdfsRootDir + scenario + "/date_yyyymmdd=" + date;
		//===============================================
		simple.setUpSparkConfig(isLocal);
		simple.process(scenario, sourceFileRootDir);

		//===============================================
		logger.info("[main] end ... Elapsed : " + (System.currentTimeMillis() - startTime) + " ms");
	}

	public void process(String scenario, String sourceFileRootDir) {
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		jsc.hadoopConfiguration().setBoolean("parquet.enable.summary-metadata", false);
		jsc.hadoopConfiguration().set("parquet.metadata.read.parallelism", "10");
		jsc.hadoopConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

		sqlContext = new SQLContext(jsc);

		//===============================================
		try {
			//JavaRDD<String> lines = sparkSession.read().format("").load(sourceFileRootDir);
			//sparkSession.read().parquet(sourceFileRootDir);
			JavaRDD<CJO_LOG_STATEFUL> baseLogRDD = sqlContext.read().format("parquet").load(sourceFileRootDir).javaRDD()
					.map(row -> parser.mapCJO_LOG_BASE(row))/*.filter(f -> (f != null))*/;
			
			JavaPairRDD<String, Iterable<CJO_LOG_STATEFUL>> groupBySid = baseLogRDD.groupBy(record -> record.getSid());
			JavaPairRDD<String, Iterable<CJO_LOG_STATEFUL>> sortedGroupBySid = groupBySid.mapValues(parser::sortLogTime);
			JavaRDD<Iterable<CJO_LOG_STATEFUL>> statefulRdd = sortedGroupBySid.values();
			CjoStatefulScenarioAnalysis anal = new CjoStatefulScenarioAnalysis();
			JavaRDD<CJO_LOG_STATEFUL_SUM> sumRdd = null;
			
			switch(scenario) {
			case "sc01" : 
				sumRdd = statefulRdd.map(anal::analysisSc01).filter(f -> (f != null));
				break;
			case "sc02" : 
				sumRdd = statefulRdd.map(anal::analysisSc02).filter(f -> (f != null));
				break;
			case "sc03" : 
				sumRdd = statefulRdd.map(anal::analysisSc03).filter(f -> (f != null));
				break;
			case "sc04" : 
				sumRdd = statefulRdd.map(anal::analysisSc04).filter(f -> (f != null));
				break;
			default :
				logger.error("invalid parameter,  scenario:{}" , scenario);
				throw new RuntimeException("invalid parameter,  scenario : " + scenario);
			}
			
			sqlContext.createDataFrame(sumRdd, CJO_LOG_STATEFUL_SUM.class)
	        .write().mode(SaveMode.Overwrite).parquet(sumHdfsRootDir);
			
		} finally {
			try {
				//sparkSession.stop();
				jsc.stop();

				//sparkConf
			} catch (Exception ex) {
				//kafkaClientProducer = null;
				ex.printStackTrace();
			}
		}
	}
/*
	public void sc01(JavaRDD<Iterable<CJO_LOG_STATEFUL>> statefulRdd) {
		CjoStatefulScenarioAnalysis anal = new CjoStatefulScenarioAnalysis();
		JavaRDD<CJO_LOG_STATEFUL_SUM> sumRdd = statefulRdd.map(anal::analysisSc01).filter(f -> (f != null));
		
		sqlContext.createDataFrame(sumRdd, CJO_LOG_STATEFUL_SUM.class)
        .write().mode(SaveMode.Overwrite).parquet(sumHdfsRootDir);
	}
	
	public void sc02(JavaRDD<Iterable<CJO_LOG_STATEFUL>> statefulRdd) {
		CjoStatefulScenarioAnalysis anal = new CjoStatefulScenarioAnalysis();
		JavaRDD<CJO_LOG_STATEFUL_SUM> sumRdd = statefulRdd.map(anal::analysisSc01).filter(f -> (f != null));
		
		sqlContext.createDataFrame(sumRdd, CJO_LOG_STATEFUL_SUM.class)
        .write().mode(SaveMode.Overwrite).parquet(sumHdfsRootDir);
	}
	
	public void sc03(JavaRDD<Iterable<CJO_LOG_STATEFUL>> statefulRdd) {
		CjoStatefulScenarioAnalysis anal = new CjoStatefulScenarioAnalysis();
		JavaRDD<CJO_LOG_STATEFUL_SUM> sumRdd = statefulRdd.map(anal::analysisSc01).filter(f -> (f != null));
		
		sqlContext.createDataFrame(sumRdd, CJO_LOG_STATEFUL_SUM.class)
        .write().mode(SaveMode.Overwrite).parquet(sumHdfsRootDir);
	}
	
	public void sc04(JavaRDD<Iterable<CJO_LOG_STATEFUL>> statefulRdd) {
		CjoStatefulScenarioAnalysis anal = new CjoStatefulScenarioAnalysis();
		JavaRDD<CJO_LOG_STATEFUL_SUM> sumRdd = statefulRdd.map(anal::analysisSc01).filter(f -> (f != null));
		
		sqlContext.createDataFrame(sumRdd, CJO_LOG_STATEFUL_SUM.class)
        .write().mode(SaveMode.Overwrite).parquet(sumHdfsRootDir);
	}
*/
	public void testSc01(JavaRDD<CJO_LOG_STATEFUL> baseLogRDD) {

		JavaPairRDD<String, Iterable<CJO_LOG_STATEFUL>> groupBySid = baseLogRDD.groupBy(record -> record.getSid());
		JavaPairRDD<String, Iterable<CJO_LOG_STATEFUL>> sortedGroupBySid = groupBySid.mapValues(parser::sortLogTime);
		//JavaRDD<Iterable<CJO_LOG_STATEFUL>> statefulRdd = sortedGroupBySid.values();
		//statefulRdd.flatMap(iter -> iter.iterator());
		sortedGroupBySid.foreachPartition(iter -> {
			Tuple2<String, Iterable<CJO_LOG_STATEFUL>> stateful = null;
			CjoStatefulScenarioAnalysis anal = new CjoStatefulScenarioAnalysis();
			
			while (iter.hasNext()) {
				stateful = iter.next();
				
				if(stateful._1.equals("U2cde982185c41044e76846916064947e91ec1579755794")) {
					anal.sc01(stateful._2);
				}
				
				//anal.sc01(iter.next()._2);
			}
		});
	}

	public void test02(JavaRDD<CJO_LOG_STATEFUL> baseLogRDD) {
		int cnt = 0;
		JavaPairRDD<String, Iterable<CJO_LOG_STATEFUL>> groupBySid = baseLogRDD.groupBy(record -> record.getSid());
		JavaPairRDD<String, Iterable<CJO_LOG_STATEFUL>> sortedGroupBySid = groupBySid.mapValues(parser::sortLogTime);
		List<Tuple2<String, Iterable<CJO_LOG_STATEFUL>>> result = sortedGroupBySid.collect();

		for (Tuple2<String, Iterable<CJO_LOG_STATEFUL>> t : result) {

			if (cnt < 3) {
				logger.info(" ======================================================== ");
				logger.info(" groupBySID: " + t._1);
				logger.info(" ======================================================== ");

				for (CJO_LOG_STATEFUL sortStateful : t._2) {
					logger.info(" stateful : {} , {} , {} , {} , {} , {}", sortStateful.getLog_time(),
							sortStateful.getCust_cd(), sortStateful.getCur_pg_info1(), sortStateful.getCur_pg_info2(),
							sortStateful.getRef_pg_info1(), sortStateful.getRef_pg_info2());
				}

				//	        while(iter.hasNext()) {
				//				stateful = iter.next();
				//				logger.info(" stateful Log_time : {}", stateful.getLog_time() );
				//			}
			}
			cnt++;
		}
		logger.info(" ======================================================== " + cnt);
		//		JavaPairRDD<String, Iterable<CJO_LOG_BASE>> baseLogRDD1 = sqlContext.read().format("parquet").load(sourceFileRootDir).javaRDD()
		//		.map(row -> parser.mapCJO_LOG_BASE(row))
		//		.groupBy(order -> order.getSid())
		//        .repartition(6)
		//        .persist(StorageLevel.MEMORY_ONLY())
		//        ;

		/*
		lines.cache();
		
		lines.filter(currentLine -> {
		
			if (currentLine == null) {
				logger.warn("Invalid (null):" + currentLine);
				return false;
			}
		
			if (currentLine.isEmpty()) {
				logger.warn("Invalid (empty):" + currentLine);
				return false;
			}
		
			try {
				dateFormat.parse(currentLine.substring(0, 23));
			} catch (ParseException | NumberFormatException pe) {
				logger.warn("Invalid (DateFormat-ParseException):" + currentLine);
				return false;
			}
		
			return true;
		
		}).foreach(filteredLine -> {
			//producerWrapper.sendEventMessage(topicName, sourceFileName.split("\\.")[0], filteredLine);
			//producerWrapper.sendEventMessage(topicName, filteredLine);
		});
		
		lines.unpersist();
		*/
	}

	public void test01(JavaRDD<CJO_LOG_STATEFUL> baseLogRDD) {
		CjoBaseLogParser parser = new CjoBaseLogParser();
		JavaPairRDD<String, CJO_LOG_STATEFUL> groupBySid = baseLogRDD
				.mapToPair(record -> new Tuple2<>(record.getSid(), record));
		JavaPairRDD<String, CJO_LOG_STATEFUL> reduceByKeyRdd = groupBySid.reduceByKey(parser::mergeVisitCount);
		List<Tuple2<String, CJO_LOG_STATEFUL>> result = groupBySid.collect();

		int cnt = 0;
		for (Tuple2<String, CJO_LOG_STATEFUL> t : result) {
			//logger.info(" groupBySID : " + t._1);
			cnt++;
		}
		logger.info(" ======================================================== " + cnt);
	}
}
