package com.joy.spark.streaming.kafka;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.internal.io.FileCommitProtocol;
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.execution.datasources.FileFormatWriter;
import org.apache.spark.sql.execution.datasources.FileFormatWriter.WriteTaskResult;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.OffsetRange;

/**
 * 
* @author joy
*
 */
public abstract class AbstractKafkaStreamAppl implements Serializable {
	
	private /*transient*/ static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractKafkaStreamAppl.class);

	public static SQLContext sqlContext;

	public Duration batchInterval = new Duration(2 * 1000);
	public String strTopics = "TEST-COMMIT-IN-TOPIC";
	public String maxRatePerPartition = "10";
	public String autoOffsetReset = "latest"; /*"earliest";*/
	
	public String groupId = "group-id-TEST-COMMIT-01";
	public String clientId = "client-id-TEST-COMMIT-01";
	
	protected abstract void start(String appName, String schemaId, String brokers, String zkHosts, String hdfsUrl,
			String hdfsSubDirColumn) throws Exception;

	protected JavaSparkContext getJavaSparkContext(String appName, String deployMode) {
		
		logger.info("▶ maxRatePerPartition:" + maxRatePerPartition + ", batchInterval:" + batchInterval);

		Class<?>[] kryoClasses = new Class<?>[] { Object[].class, GenericRow.class, FileFormatWriter.class, WriteTaskResult.class,
				FileCommitProtocol.class, TaskCommitMessage.class };

		//spark conf
		SparkConf sc = new SparkConf().setAppName(appName);
		if (deployMode != null && deployMode.equals("local")) {
		
			sc.setMaster("local[2]");

		}// else {
			sc.set("spark.driver.cores", "1");
			sc.set("spark.driver.memory", "1G");
			sc.set("spark.driver.maxResultSize", "1G");
			sc.set("spark.executor.instances", "1");
			sc.set("spark.executor.cores", "1");
			sc.set("spark.executor.memory", "1G");
			sc.set("spark.executor.heartbeatInterval", "3000");
			
			// for Spark Streaming Graceful Shutdown
			sc.set("spark.streaming.stopGracefullyOnShutdown", "true");
		//}
		
		sc.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc.set("spark.kryo.registrationRequired", "true");
		sc.registerKryoClasses(kryoClasses);

		// for Spark Streaming BackPressure
		sc.set("spark.streaming.backpressure.enabled", "true");
		
		sc.set("spark.sql.hive.convertMetastoreParquet", "false");
		sc.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition); // kafka partition * 30000
		
		//java spark context
		JavaSparkContext jsc = new JavaSparkContext(sc);
		jsc.hadoopConfiguration().setBoolean("parquet.enable.summary-metadata", false);
		jsc.hadoopConfiguration().set("parquet.metadata.read.parallelism", "10");
		jsc.hadoopConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

		return jsc;
	}

	protected Map<String, Object> getKafkaParams(String brokers, Class<?> keyDeserializer,
			Class<?> valueDeserializer) {

		//Kafka configuration
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("key.deserializer", keyDeserializer);
		kafkaParams.put("value.deserializer", valueDeserializer);
		kafkaParams.put("group.id", groupId);
		//kafkaParams.put("client.id", clientId);
		kafkaParams.put("auto.offset.reset", autoOffsetReset);
		kafkaParams.put("enable.auto.commit", false);

		// 2018-08-02
		// for streaming-kafka-0-10-integration
		// request.timeout.ms should be greater than session.timeout.ms and fetch.max.wait.ms
		// request.timeout.ms (default:40000) v0.10.0.x
		// session.timeout.ms (default:30000) v0.10.0.x
		if (batchInterval.milliseconds() >= 300000) {
			logger.warn("#####################################################################################################");
			
			logger.warn(
					"▶ [getKafkaParams] batchInterval is over 300,000 ms. For batches larger than 5 minutes, This will require changing group.max.session.timeout.ms on the broker! , batchInterval : "
							+ batchInterval.milliseconds());
			logger.warn(
					"▶ [getKafkaParams] Check the following site , https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html");
			
			logger.warn("#####################################################################################################");
		}

		if (batchInterval.milliseconds() >= 30000) {
			logger.warn(
					"▶ [getKafkaParams] batchInterval is over 30,000 ms. So KafkaParams will be reset! , batchInterval : "
							+ batchInterval.milliseconds());
			logger.warn("▶ [getKafkaParams] session.timeout.ms : " + (int)batchInterval.milliseconds());
			logger.warn("▶ [getKafkaParams] heartbeat.interval.ms : " + (int)(batchInterval.milliseconds()/10));
			logger.warn("▶ [getKafkaParams] request.timeout.ms : " + (int)(batchInterval.milliseconds() + 10000));
			
			kafkaParams.put("session.timeout.ms", (int)(batchInterval.milliseconds()));
			kafkaParams.put("heartbeat.interval.ms", (int)(batchInterval.milliseconds()/10));
			
			// if not setting, the following error will occurs.
			// org.apache.kafka.common.config.ConfigException: 
			// request.timeout.ms should be greater than session.timeout.ms and fetch.max.wait.ms
			kafkaParams.put("request.timeout.ms", (int)(batchInterval.milliseconds() + 10000));
		}
		
		return kafkaParams;
	}

	/**
	 * 2018-07-31
	 * stop spark streaming application gracefully
	 * replaced by --conf "spark.streaming.stopGracefullyOnShutdown=true"
	 * 
	 * @Deprecated this approach does not work in new Spark version (after Spark 1.4). It will cause deadlock situation
	* http://blog.parseconsulting.com/2017/02/how-to-shutdown-spark-streaming-job.html
	* @param jssc
	 */
	@Deprecated
	protected void stopGracefullyOnShutdown(JavaStreamingContext jssc, KafkaConsumer kafkaConsumer) {

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				logger.warn("Gracefully stopping Spark Streaming Application : kafkaConsumer.close()");
				String stopGracefullyOnShutdown = null;
				try {
					stopGracefullyOnShutdown = jssc.sparkContext().getConf()
							.get("spark.streaming.stopGracefullyOnShutdown");
				} catch (NoSuchElementException ex) {
					logger.warn("[stopGracefullyOnShutdown] : " + ex.toString());
				}

				if(kafkaConsumer != null) {
					kafkaConsumer.close();
				}
				
				logger.info("▶ stopGracefullyOnShutdown config : " + stopGracefullyOnShutdown);
				if (stopGracefullyOnShutdown == null || stopGracefullyOnShutdown.equals("false")) {
					logger.warn("▶ started Runtime.getRuntime().addShutdownHook for stopGracefullyOnShutdown config : "
							+ stopGracefullyOnShutdown);
					jssc.stop(true, true);
				}
				logger.warn("Spark Streaming Application stopped : kafkaConsumer.close()");
			}
		});
	}

	/*
	 * for debugging
	 */
	protected void printOffsetRangeInfo(OffsetRange[] offsetRangeArray, String prefix) {
		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
		offsetRanges.set(offsetRangeArray);

		for (OffsetRange offset : offsetRanges.get()) {
			logger.debug("[OffsetRangeInfo] >>> prefix : " + prefix + " , topic=" + offset.topic() + ", partition="
					+ offset.partition() + ", fromOffset=" + offset.fromOffset() + ", lastOffset="
					+ offset.untilOffset());
		}
	}
}
