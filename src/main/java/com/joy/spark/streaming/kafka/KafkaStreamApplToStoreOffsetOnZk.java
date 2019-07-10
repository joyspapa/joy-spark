package com.joy.spark.streaming.kafka;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

/**
 * 
* @author joy
*
 */
public class KafkaStreamApplToStoreOffsetOnZk extends AbstractKafkaStreamAppl {

	// for log4j
	//private transient final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(this.getClass());

	private /*transient*/static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KafkaStreamApplToStoreOffsetOnZk.class);

	public static void main(String[] args) throws Exception {
		new KafkaStreamApplToStoreOffsetOnZk().start("TEST-COMMIT", "/logplanet/DE1532051288/ENT2577",
				"192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092",
				"192.168.10.82:2181,192.168.10.83:2181,192.168.10.84:2181", "hdfs://obz-hadoop-ha/user/ecube", "local");
	}

	public void start(String appName, String schemaId, String brokers, String zkHosts, String hdfsUrl,
			String deployMode) throws Exception {
		logger.info("▶ appName:" + appName + "schemaId:" + schemaId + ", hdfsUrl:" + hdfsUrl + ", brokers:" + brokers
				+ ", zkHosts:" + zkHosts + ", deployMode [optional]:" + deployMode);

		// for local testing
		JavaSparkContext jsc = getJavaSparkContext(appName, deployMode);

		// get SparkContext
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, batchInterval);

		// get Kafka configuration
		Map<String, Object> kafkaParams = getKafkaParams(brokers, StringDeserializer.class, StringDeserializer.class);

		// create Kafka
		//final KafkaConsumer kafkaConsumer = new KafkaConsumer<>(kafkaParams);

		// https://github.com/Jiwei0/javalearning/blob/master/src/main/java/com/me/demo/spark/UserClickCountAnalytics.java
		// create zkClient
		ZkClient zkClient = new ZkClient(zkHosts);
		List<String> topicList = Arrays.asList(strTopics.split(","));
		Map<TopicPartition, Long> fromOffsets = new HashMap<>();
		String zkGroupIdPath = "/logplanet/offset/" + groupId + "/";

		for (String topic : topicList) {
			String zkTopicPath = zkGroupIdPath + topic;
			int partitionLength = zkClient.countChildren(zkTopicPath);
			logger.info("▶ zkTopicPath : " + zkTopicPath + " , partitionLength : " + partitionLength);

			if (partitionLength > 0) {
				for (int i = 0; i < partitionLength; i++) {
					String partitionOffset = zkClient.readData(zkTopicPath + "/" + i, false);
					//long partitionOffset = zkClient.readData(zkTopicPath + "/" + i, false);
					logger.info("▶ patition : " + i + " , partitionOffset : " + partitionOffset);

					TopicPartition topicPartition = new TopicPartition(topic, i);
					fromOffsets.put(topicPartition, Long.parseLong(partitionOffset));
				}
			}
		}

		JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = getKafkaDStream(jssc, fromOffsets, kafkaParams);

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

				// happened Error
				//commitSyncOffsetIntoKafka(kafkaConsumer, offsetRanges);

				// for debugging
				saveOffsetIntoZK(zkClient, zkGroupIdPath, offsetRanges);

				if (logger.isDebugEnabled()) {
					printOffsetRangeInfo(offsetRanges, "after commitSync ");
				}
			}
		});

		// Start the computation
		jssc.start();

		// stop spark streaming application gracefully
		//stopGracefullyOnShutdown(jssc, /*kafkaConsumer*/null);
		
		// Wait for the computation to terminate
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			logger.error("Application awaitTermination ERROR : ");
		}
	}

	protected JavaInputDStream<ConsumerRecord<String, String>> getKafkaDStream(JavaStreamingContext jssc,
			Map<TopicPartition, Long> fromOffsets, Map<String, Object> kafkaParams) {
		JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = null;

		if (fromOffsets.isEmpty()) {
			kafkaStream = KafkaUtils.createDirectStream(jssc, //streaming context
					LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(
							new HashSet<>(Arrays.asList(strTopics.split(","))), kafkaParams));

			logger.info("▶ Create kafka direct stream... topic : " + strTopics);
		} else {
			kafkaStream = KafkaUtils.createDirectStream(jssc, //streaming context
					LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(
							new HashSet<>(Arrays.asList(strTopics.split(","))), kafkaParams, fromOffsets));

			logger.info("▶ Create kafka direct stream... with fromOffsets topic : " + strTopics);
		}

		return kafkaStream;
	}

	// Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member
	protected void commitSyncOffsetIntoKafka(KafkaConsumer kafkaConsumer, OffsetRange[] offsetRangeArray) {
		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
		offsetRanges.set(offsetRangeArray);

		Map<TopicPartition, OffsetAndMetadata> fromOffsets = new HashMap<>();

		for (OffsetRange offsetRange : offsetRanges.get()) {
			TopicPartition topicPartition = new TopicPartition(offsetRange.topic(), offsetRange.partition());
			OffsetAndMetadata offsetMeta = new OffsetAndMetadata(offsetRange.untilOffset());
			fromOffsets.put(topicPartition, offsetMeta);
		}

		kafkaConsumer.commitSync(fromOffsets);
	}

	protected void saveOffsetIntoZK(ZkClient zkClient, String zkGroupIdPath, OffsetRange[] offsetRangeArray) {
		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
		offsetRanges.set(offsetRangeArray);

		for (OffsetRange offsetRange : offsetRanges.get()) {
			String zkTopicPath = zkGroupIdPath + offsetRange.topic() + "/" + offsetRange.partition();
			if (zkClient.exists(zkTopicPath)) {
				zkClient.writeData(zkTopicPath, String.valueOf(offsetRange.untilOffset()));
				logger.info("[saveOffsetOnZK] saved offset : " + offsetRange.untilOffset());
			} else {
				zkClient.createPersistent(zkTopicPath, true);
				try {
					zkClient.writeData(zkTopicPath, String.valueOf(offsetRange.untilOffset()));
					logger.info("[saveOffsetOnZK] saved offset after creating zkNode : " + String.valueOf(offsetRange.untilOffset()));
				} catch (Exception e) {
					logger.warn("[saveOffsetOnZK] zkClient.writeData ERROR : " + e.toString());
				}
			}
		}
	}

}