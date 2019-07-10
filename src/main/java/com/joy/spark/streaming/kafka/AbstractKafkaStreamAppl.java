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

import com.joy.spark.streaming.properties.AutoOffsetResetType;
import com.joy.spark.streaming.properties.KafkaProperty;
import com.joy.spark.streaming.properties.KafkaStreamApplProperty;
import com.joy.spark.streaming.properties.SparkProperty;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * 
* @author joy
*
 */
public abstract class AbstractKafkaStreamAppl implements Serializable {

	private /*transient*/ static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
			.getLogger(AbstractKafkaStreamAppl.class);
	boolean isLocalMode = true;

	public static SQLContext sqlContext;

	//protected abstract void start(String appName, String schemaId, String brokers, String zkHosts, String hdfsUrl)
	//		throws Exception;

	protected abstract void start(KafkaStreamApplProperty prop)
			throws Exception;
	
	protected void process(String[] args) throws Exception {

		if (args != null && args.length > 0) {
			OptionParser parser = new OptionParser();
			//mandatory arguments
			OptionSpec<String> appName = parser.accepts("appName").withRequiredArg().ofType(String.class)
					.describedAs("Application Name");
			OptionSpec<String> schemaIdOp = parser.accepts("schema").withRequiredArg().ofType(String.class)
					.describedAs("Schema ID");
			OptionSpec<String> brokersOp = parser.accepts("brokers").withRequiredArg().ofType(String.class)
					.describedAs("Kafka borkers");
			OptionSpec<String> zkHostsOp = parser.accepts("zkHosts").withRequiredArg().ofType(String.class)
					.describedAs("ZooKeeper hosts");
			OptionSpec<String> hdfsUrlOp = parser.accepts("hdfsUrl").withRequiredArg().ofType(String.class)
					.describedAs("HDFS URL");

			OptionSet optionSet = parser.parse(args);
			if (!(optionSet.has(appName) && optionSet.has(brokersOp) && optionSet.hasArgument(zkHostsOp)
					&& optionSet.hasArgument(schemaIdOp) && optionSet.hasArgument(hdfsUrlOp))) {
				parser.printHelpOn(System.out);
				logger.error("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
				logger.error("▶ essential arguments are required!");
				logger.error("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
				System.exit(0);
			}

			isLocalMode = false;

			// call start
			//start(optionSet.valueOf(appName), optionSet.valueOf(schemaIdOp), optionSet.valueOf(brokersOp),
			//		optionSet.valueOf(zkHostsOp), optionSet.valueOf(hdfsUrlOp));
			
			KafkaStreamApplProperty prop = new KafkaStreamApplProperty();
			
			// ------------------------------------------------------
			// set Spark Properties
			prop.getSparkProp().setSparkAppName(optionSet.valueOf(appName));
			prop.getSparkProp().setBatchIntervalMilis(new Duration(2 * 1000));
			// ------------------------------------------------------
						
			// ------------------------------------------------------
			// set Kafka Properties
			prop.getKafkaProp().setBrokers(optionSet.valueOf(brokersOp));
			prop.getKafkaProp().setTopics("TEST-COMMIT-IN-TOPIC");
			prop.getKafkaProp().setAutoOffsetReset(AutoOffsetResetType.EARLIEST);
			prop.getKafkaProp().setGroupId("group-id-default");
			prop.getKafkaProp().setClientId("client-id-default");
			// ------------------------------------------------------

			prop.setSchemaId(optionSet.valueOf(schemaIdOp));
			prop.setZkHosts(optionSet.valueOf(zkHostsOp));
			prop.setHdfsUrl(optionSet.valueOf(hdfsUrlOp));
			
			start(prop);
		}
	}

	protected JavaSparkContext getJavaSparkContext(SparkProperty sparkProp) {

		logger.info("▶▶ maxRatePerPartition:" + sparkProp.getMaxRatePerPartition() + ", batchInterval:"
				+ sparkProp.getBatchIntervalMilis());

		//spark conf
		SparkConf sc = new SparkConf().setAppName(sparkProp.getSparkAppName());

		if (isLocalMode) {
			sc.setMaster("local[*]"); // in case local test, enable this.
		}

		sc.set("spark.driver.cores", sparkProp.getSparkDriverCores());
		sc.set("spark.driver.memory", sparkProp.getSparkDriverMemory());
		sc.set("spark.driver.maxResultSize", sparkProp.getSparkDriverMaxResultSize());
		sc.set("spark.executor.instances", sparkProp.getSparkExecutorInstances());
		sc.set("spark.executor.cores", sparkProp.getSparkExecutorCores());
		sc.set("spark.executor.memory", sparkProp.getSparkExecutorMemory());
		sc.set("spark.executor.heartbeatInterval", sparkProp.getSparkExecutorHeartbeatInterval());
		sc.set("spark.streaming.kafka.maxRatePerPartition", sparkProp.getMaxRatePerPartition()); // kafka partition * maxRatePerPartition

		// for Spark Streaming Graceful Shutdown
		sc.set("spark.streaming.stopGracefullyOnShutdown", "true");

		// ---------------------
		// Spark Tuning Option
		// ---------------------
		sc.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sc.set("spark.kryo.registrationRequired", "true");
		sc.set("spark.sql.hive.convertMetastoreParquet", "false");
		sc.set("spark.streaming.backpressure.enabled", "true"); // for Spark Streaming BackPressure

		Class<?>[] kryoClasses = new Class<?>[] { Object[].class, GenericRow.class, FileFormatWriter.class,
				WriteTaskResult.class, FileCommitProtocol.class, TaskCommitMessage.class };
		sc.registerKryoClasses(kryoClasses);

		//java spark context
		JavaSparkContext jsc = new JavaSparkContext(sc);
		jsc.hadoopConfiguration().setBoolean("parquet.enable.summary-metadata", false);
		jsc.hadoopConfiguration().set("parquet.metadata.read.parallelism", "10");
		jsc.hadoopConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

		return jsc;
	}

	protected Map<String, Object> getKafkaParams(KafkaProperty kafkaProp, SparkProperty sparkProp) {

		//Kafka configuration
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", kafkaProp.getBrokers());
		kafkaParams.put("key.deserializer", kafkaProp.getKeyDeserializer());
		kafkaParams.put("value.deserializer", kafkaProp.getValueDeserializer());
		kafkaParams.put("group.id", kafkaProp.getGroupId());
		kafkaParams.put("client.id", kafkaProp.getClientId());
		kafkaParams.put("auto.offset.reset", kafkaProp.getAutoOffsetReset());
		kafkaParams.put("enable.auto.commit", kafkaProp.isEnableAutoCommit());

		// 2018-08-02
		// for streaming-kafka-0-10-integration
		// request.timeout.ms should be greater than session.timeout.ms and fetch.max.wait.ms
		// request.timeout.ms (default:40000) v0.10.0.x
		// session.timeout.ms (default:30000) v0.10.0.x
		if (sparkProp.getBatchIntervalMilis().milliseconds() >= 300000) {
			logger.warn(
					"#####################################################################################################");

			logger.warn(
					"▶ [getKafkaParams] batchInterval is over 300,000 ms. For batches larger than 5 minutes, This will require changing group.max.session.timeout.ms on the broker! , batchInterval : "
							+ sparkProp.getBatchIntervalMilis().milliseconds());
			logger.warn(
					"▶ [getKafkaParams] Check the following site , https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html");

			logger.warn(
					"#####################################################################################################");
		}

		if (sparkProp.getBatchIntervalMilis().milliseconds() >= 30000) {
			long batchIntervalMilis = sparkProp.getBatchIntervalMilis().milliseconds();
			logger.warn(
					"▶ [getKafkaParams] batchInterval is over 30,000 ms. So KafkaParams will be reset! , batchInterval : "
							+ batchIntervalMilis);
			logger.warn("▶ [getKafkaParams] session.timeout.ms : " + (int) batchIntervalMilis);
			logger.warn("▶ [getKafkaParams] heartbeat.interval.ms : " + (int) (batchIntervalMilis / 10));
			logger.warn("▶ [getKafkaParams] request.timeout.ms : " + (int) (batchIntervalMilis + 10000));

			kafkaParams.put("session.timeout.ms", (int) (batchIntervalMilis));
			kafkaParams.put("heartbeat.interval.ms", (int) (batchIntervalMilis / 10));

			// if not setting, the following error will occurs.
			// org.apache.kafka.common.config.ConfigException: 
			// request.timeout.ms should be greater than session.timeout.ms and fetch.max.wait.ms
			kafkaParams.put("request.timeout.ms", (int) (batchIntervalMilis + 10000));
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

				if (kafkaConsumer != null) {
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
