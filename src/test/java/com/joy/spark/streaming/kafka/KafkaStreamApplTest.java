package com.joy.spark.streaming.kafka;

import org.apache.spark.streaming.Duration;
import org.junit.Test;

import com.joy.spark.streaming.properties.AutoOffsetResetType;
import com.joy.spark.streaming.properties.KafkaStreamApplProperty;

public class KafkaStreamApplTest {

	@Test
	public void test() throws Exception {
		KafkaStreamApplProperty prop = new KafkaStreamApplProperty();
		// ------------------------------------------------------
		// set Spark Properties
		prop.getSparkProp().setSparkAppName("TEST-COMMIT");
		prop.getSparkProp().setBatchIntervalMilis(new Duration(2 * 1000));
		
		prop.getSparkProp().setSparkExecutorCores("2");
		prop.getSparkProp().setSparkExecutorInstances("2");
		// ------------------------------------------------------

		// ------------------------------------------------------
		// set Kafka Properties
		prop.getKafkaProp().setBrokers("192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092");
		prop.getKafkaProp().setTopics("TEST-COMMIT-IN-TOPIC");
		prop.getKafkaProp().setAutoOffsetReset(AutoOffsetResetType.EARLIEST);
		prop.getKafkaProp().setGroupId("group-id-default");
		prop.getKafkaProp().setClientId("client-id-default");
		// ------------------------------------------------------

		prop.setSchemaId("/logplanet/DE1532051288/ENT2577");
		prop.setZkHosts("192.168.10.82:2181,192.168.10.83:2181,192.168.10.84:2181");
		prop.setHdfsUrl("hdfs://obz-hadoop-ha/user/ecube");

		new KafkaStreamAppl().start(prop);
	}
}
