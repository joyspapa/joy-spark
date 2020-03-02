package com.obzen.spark.application.batch.cjo;

import java.io.Serializable;
import java.text.SimpleDateFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hanmin on 17. 3. 2.
 */
public class FileToKafkaBatchCJO_20200121 implements Serializable {
	private static final long serialVersionUID = 8965924708608823997L;
	private static final Logger logger = LoggerFactory.getLogger(FileToKafkaBatchCJO_20200121.class);
	private static SparkSession sparkSession;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

	private static String topicPrefix = "-IN-TOPIC";

	private static String fileNames = "click_pc_web,click_mobile_webview,click_mobile_web,tracker_mobile_app,tracker_mobile_webview,tracker_mobile_web,tracker_pc_web";

	public void setUpSparkConfig(boolean isLocal) {
		SparkConf sparkConf = new SparkConf().setAppName("CJ OShopping Spark Batch Application");
		sparkConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		//sparkConf.registerKryoClasses(new Class<?>[] { KafkaProducerWrapperCJO.class/*, CJmallLogVO.class*/ });

		Builder sparkBuilder = SparkSession.builder().appName("FileToKafkaBatchTest")
				.config("parquet.enable.summary-metadata", false).config("parquet.metadata.read.parallelism", "10")
				.config("mapreduce.fileoutputcommitter.marksuccessfuljobs", false).config(sparkConf);

		if (isLocal) {
			sparkBuilder.master("local[*]");
		}

		sparkSession = sparkBuilder.getOrCreate();
	}

	public static void main(String[] args) throws Exception {
		String topic = "CJO-LOG-IN-TOPIC";

		logger.info("[main] begin ...");
		long startTime = System.currentTimeMillis();

		FileToKafkaBatchCJO_20200121 simple = new FileToKafkaBatchCJO_20200121();

		String sourceFileRootDir = "C:\\working\\201710\\head_10_line\\";
		String sourceFileName = "click_pc_web.log.2017-10-05_10";

		if (args[0].equals("local")) {
			logger.info("[FileBatchMain] local ...");
			//sourceFileName = "C:\\working\\splunk_log_1day\\tracker_mobile_clickzone.log.2017-09-12";
			sourceFileName = "tracker_mobile_app.log.2017-10-05_10";
			//sourceFileName = "click_mobile_webview.log.2017-10-05";

			topic = "TRACKER_MOBILE_APP-IN-TOPIC";
			
		} else if ((args.length == 1 && args[0].contains(".log."))) {
			boolean isExistTopic = false;
			sourceFileRootDir = "/user/ecube/obzen/batch/indata/";

			for (String fileName : fileNames.split(",", -1)) {
				if (args[0].contains(fileName)) {
					topic = fileName.toUpperCase() + topicPrefix;
					isExistTopic = true;
					break;
				}
			}

			if (!isExistTopic) {
				throw new IllegalArgumentException(
						"Check Topic Name about the File Name! [ Parameters must be a file name. ex) click_pc_web.log.2017-10-05 ] : "
								+ args.toString());
			}

			logger.info("topic name : " + topic);

			sourceFileName = args[0];

		} else if (args.length >= 3 && args[1].contains("-")  && args[2].endsWith(".log")) {
			boolean isExistTopic = false;
			sourceFileRootDir = "/user/ecube/obzen/batch/indata/"+args[0]+"/" +args[1]+"/" ;

			logger.warn(">>> args[0] : " + args[0] + ", args[1] : " + args[1] + ", args[2] : " + args[2]);
			
			for (String fileName : fileNames.split(",", -1)) {
				
				if (args[2].contains(fileName)) {
					topic = fileName.toUpperCase() + topicPrefix;
					isExistTopic = true;
					break;
				}
			}

			if (!isExistTopic) {
				throw new IllegalArgumentException(
						"Check Topic Name about the File Name! [ Parameters must be a log_server_name, date, FILE_NAME, ex)server1 2017-10-05 click_pc_web.log ] : "
								);
			}

			sourceFileName = args[2] + "." + args[1];
			
			logger.info(">>> topic name : " + topic + ", sourceFileName name : " + sourceFileName);

			
		} else {
			throw new IllegalArgumentException(
					"Check Parameter! [ Parameters must be a file name. ex) click_pc_web.log.2017-10-05 ] : "
							+ args.toString());
		}

		//===============================================
		try {
			simple.setUpSparkConfig(args[0].equals("local"));

			if(args[0].equals("local")) {
				simple.readTest(sourceFileRootDir, sourceFileName);
				return;
			}
			
			simple.process(sourceFileRootDir, sourceFileName, topic);
			
		} finally {
			try {
				
				sparkSession.stop();
			} catch (Exception ex) {
				//kafkaClientProducer = null;
				ex.printStackTrace();
			}
		}

		//===============================================
		logger.info("[main] end ... Elapsed : " + (System.currentTimeMillis() - startTime) + " ms");

		//Thread.sleep(30000);

	}

	/*
	public void processOld(String sourceFileRootDir, String sourceFileName, String topicName,
			KafkaProducerWrapperTest producerWrapper) {

		JavaRDD<String> lines = sparkSession.read().textFile(sourceFileRootDir + sourceFileName).javaRDD();
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
			producerWrapper.sendEventMessage(topicName, filteredLine);
		});

		lines.unpersist();
	}
	*/
	
	public void process(String sourceFileRootDir, String sourceFileName, String topicName) {

		JavaRDD<String> lines = sparkSession.read().textFile(sourceFileRootDir + sourceFileName).javaRDD();
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

//			try {
//				dateFormat.parse(currentLine.substring(0, 23));
//			//} catch (ParseException | NumberFormatException pe) {
//			} catch (Exception pe) {
//				logger.warn("Invalid (DateFormat-ParseException):" + currentLine);
//				return false;
//			}

			return true;

		}).foreachPartition(partition -> {
			//int keyIndex = 0;
			while (partition.hasNext()) {
				
				StringBuilder concatDateField = new StringBuilder();
				concatDateField.append(partition.next());
				
				if (topicName.toLowerCase().contains("click_pc_web")) {
					concatDateField.append(", ");
					concatDateField.append("host=\"web_click\", ");
					concatDateField.append("sourcetype=\"CPCW\"");

				} else if (topicName.toLowerCase().contains("click_mobile_webview")) {
					concatDateField.append(", ");
					concatDateField.append("host=\"web_click\", ");
					concatDateField.append("sourcetype=\"CMWV\"");

				} else if (topicName.toLowerCase().contains("click_mobile_web")) {
					concatDateField.append(", ");
					concatDateField.append("host=\"web_click\", ");
					concatDateField.append("sourcetype=\"CMOW\"");

				} else if (topicName.toLowerCase().contains("tracker_pc_web")) {
					concatDateField.append(", ");
					concatDateField.append("host=\"m_logger\", ");
					concatDateField.append("sourcetype=\"TPCW\"");

				} else if (topicName.toLowerCase().contains("tracker_mobile_webview")) {
					concatDateField.append(", ");
					concatDateField.append("host=\"m_logger\", ");
					concatDateField.append("sourcetype=\"TMWV\"");

				} else if (topicName.toLowerCase().contains("tracker_mobile_web")) {
					concatDateField.append(", ");
					concatDateField.append("host=\"m_logger\", ");
					concatDateField.append("sourcetype=\"TMOW\"");

				} else if (topicName.toLowerCase().contains("tracker_mobile_app")) {
					concatDateField.append(", ");
					concatDateField.append("host=\"app_logger\", ");
					concatDateField.append("sourcetype=\"TAPP\"");

				}
				
				//producerWrapper.sendEventMessage("MIG_CORE_LOG_IN_TOPIC", /*topicName+"_"+String.valueOf(keyIndex++),*/ concatDateField.toString());
				
				//producerWrapper.sendEventMessage(topicName, /*topicName+"_"+String.valueOf(keyIndex++),*/ partition.next());
			}
		});

		lines.unpersist();
	}
	/*
	public void processBulk(String sourceFileRootDir, String sourceFileName, String topicName,
			KafkaProducerWrapperTest producerWrapper) {

		JavaRDD<String> lines = sparkSession.read().textFile(sourceFileRootDir + sourceFileName).javaRDD();
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

		}).foreachPartition(partition -> {
			List<KeyedMessage> listMessage = new ArrayList<KeyedMessage>();
			//KeyedMessage message = null;
			int keyIndex = 0;
			while (partition.hasNext()) {
				listMessage.add(new KeyedMessage(topicName, partition.next()));
			}
			producerWrapper.sendEventMessage(topicName, listMessage);
			
			listMessage.clear();
		});

		lines.unpersist();
	}
	*/
	
	public void readTest(String sourceFileName, String outputFileName) throws Exception {

		sourceFileName = "hdfs://210.122.104.136:8020/user/hduser/obzen/parquet/CJO-LOG-HDFS/CJO-LOG/*";
		//sourceFileName = "C:\\working\\201709\\output\\app\\*";

		Dataset<Row> readDataset = sparkSession.read().parquet(sourceFileName);
		readDataset.cache();
		
		//readDataset.show();
//		readDataset.createOrReplaceTempView("cjo_log");
//		
//		StringBuilder sql = new StringBuilder();
//		sql.append("select uid, cust_cd, cust_id from cjo_log ");
//		sql.append("where ");
//		sql.append("sourcetype='CMWV' ");
//		sql.append("AND uid='Ubc6d1a30a7e10f4278b8fab941a6adcf857c1507173708' ");
//		sql.append("limit 10");
//		
//		Dataset<Row> resultDataset = readDataset.sqlContext().sql(sql.toString());
//		
//		resultDataset.show();
		
		//readDataset.select("count").show(200);

		//logger.info("[readTest] readDataset.toJavaRDD().getNumPartitions() : "
		// + readDataset.toJavaRDD().getNumPartitions());
		logger.info("[readTest] readDataset.count() : " + readDataset.count());

	}

}
