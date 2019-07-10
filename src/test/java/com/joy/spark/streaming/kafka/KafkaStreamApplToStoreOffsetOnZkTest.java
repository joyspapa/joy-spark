package com.joy.spark.streaming.kafka;

import org.junit.Test;

public class KafkaStreamApplToStoreOffsetOnZkTest {

	@Test
	public void test() throws Exception {
		new KafkaStreamApplToStoreOffsetOnZk().start("TEST-COMMIT",
				"/logplanet/DE1532051288/ENT2577", "192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092",
				"192.168.10.82:2181,192.168.10.83:2181,192.168.10.84:2181", "hdfs://obz-hadoop-ha/user/ecube", "local");
	}
}
