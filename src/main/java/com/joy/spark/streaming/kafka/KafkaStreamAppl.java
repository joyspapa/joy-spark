package com.joy.spark.streaming.kafka;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.joy.spark.streaming.properties.KafkaStreamApplProperty;

/**
 * 
* @author joy
*
 */
public class KafkaStreamAppl extends AbstractKafkaStreamAppl {

	// for log4j
	//private transient final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(this.getClass());
	private /*transient*/static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
			.getLogger(KafkaStreamAppl.class);

	public static void main(String[] args) throws Exception {
		new KafkaStreamAppl().process(args);
	}

	public void start(KafkaStreamApplProperty prop) throws Exception {
		logger.info("▶ appName:" + prop.getSparkProp().getSparkAppName() + ", schemaId:" + prop.getSchemaId()
				+ ", hdfsUrl:" + prop.getHdfsUrl() + ", brokers:" + prop.getKafkaProp().getBrokers() + ", zkHosts:"
				+ prop.getZkHosts());

		logger.warn("▶ isLocalMode = " + isLocalMode);
		
		// for local testing
		JavaSparkContext jsc = getJavaSparkContext(prop.getSparkProp());

		// get SparkContext
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, prop.getSparkProp().getBatchIntervalMilis());

		// get Kafka configuration
		Map<String, Object> kafkaParams = getKafkaParams(prop.getKafkaProp(), prop.getSparkProp());

		JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(jssc, //streaming context
				LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(
						new HashSet<>(Arrays.asList(prop.getKafkaProp().getTopics().split(","))), kafkaParams));

		logger.info("▶ Create kafka direct stream... topic : " + prop.getKafkaProp().getTopics());

		kafkaStream.foreachRDD(kafkaStreamRDD -> {
			if (logger.isDebugEnabled()) {
				logger.debug("▶▷ foreachRDD starting ... : ");
			}

			if (!kafkaStreamRDD.isEmpty()) {
				// get offsetRanges
				OffsetRange[] offsetRanges = ((HasOffsetRanges) kafkaStreamRDD.rdd()).offsetRanges();

				//================================================
				// User Code Area

				kafkaStreamRDD.foreach(consumerRecord -> {
					logger.info("reteived message : " + consumerRecord.value());
				});

				// User Code Area
				//================================================

				// commit offsetRanges
				((CanCommitOffsets) kafkaStream.inputDStream()).commitAsync(offsetRanges);

				if (logger.isDebugEnabled()) {
					printOffsetRangeInfo(offsetRanges, "after commitSync ");
				}
			}
		});

		// Start the computation
		jssc.start();

		if (isLocalMode) {
			// stop spark streaming application gracefully
			stopGracefullyOnShutdown(jssc, /*kafkaConsumer*/null);
		}

		// Wait for the computation to terminate
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			logger.error("Application awaitTermination ERROR : ");
		}
	}

}