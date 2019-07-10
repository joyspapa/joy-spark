package com.joy.spark.streaming.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.joy.spark.streaming.properties.KafkaProperty;
import com.joy.spark.streaming.properties.KafkaStreamApplProperty;

/**
 * 
* @author joy
*
 */
public class KafkaStreamApplToStoreOffsetOnZk extends AbstractKafkaStreamAppl {

	// for log4j
	//private transient final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(this.getClass());
	private /*transient*/static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KafkaStreamApplToStoreOffsetOnZk.class);

	public void start(KafkaStreamApplProperty prop) throws Exception {
		logger.info("▶ appName:" + prop.getSparkProp().getSparkAppName() + ", schemaId:" + prop.getSchemaId()
		+ ", hdfsUrl:" + prop.getHdfsUrl() + ", brokers:" + prop.getKafkaProp().getBrokers() + ", zkHosts:"
		+ prop.getZkHosts());

		// for local testing
		JavaSparkContext jsc = getJavaSparkContext(prop.getSparkProp());

		// get SparkContext
		JavaStreamingContext jssc = new JavaStreamingContext(jsc, prop.getSparkProp().getBatchIntervalMilis());

		// get Kafka configuration
		Map<String, Object> kafkaParams = getKafkaParams(prop.getKafkaProp(), prop.getSparkProp());

		// create Kafka
		//final KafkaConsumer kafkaConsumer = new KafkaConsumer<>(kafkaParams);

		// https://github.com/Jiwei0/javalearning/blob/master/src/main/java/com/me/demo/spark/UserClickCountAnalytics.java
		// create zkClient
		ZkClient zkClient = new ZkClient(prop.getZkHosts());
		List<String> topicList = Arrays.asList(prop.getKafkaProp().getTopics().split(","));
		Map<TopicPartition, Long> fromOffsets = new HashMap<>();
		String zkGroupIdPath = "/logplanet/offset/" + prop.getKafkaProp().getGroupId() + "/";

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

		JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = getKafkaDStream(jssc, fromOffsets, kafkaParams, prop.getKafkaProp());

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
			Map<TopicPartition, Long> fromOffsets, Map<String, Object> kafkaParams, KafkaProperty kafkaProp) {
		JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = null;

		if (fromOffsets.isEmpty()) {
			kafkaStream = KafkaUtils.createDirectStream(jssc, //streaming context
					LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(
							new HashSet<>(Arrays.asList(kafkaProp.getTopics().split(","))), kafkaParams));

			logger.info("▶ Create kafka direct stream... topic : " + kafkaProp.getTopics());
		} else {
			kafkaStream = KafkaUtils.createDirectStream(jssc, //streaming context
					LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String> Subscribe(
							new HashSet<>(Arrays.asList(kafkaProp.getTopics().split(","))), kafkaParams, fromOffsets));

			logger.info("▶ Create kafka direct stream... with fromOffsets topic : " + kafkaProp.getTopics());
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