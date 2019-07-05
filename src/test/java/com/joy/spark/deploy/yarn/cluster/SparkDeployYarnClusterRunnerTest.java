package com.joy.spark.deploy.yarn.cluster;

import org.junit.Test;

public class SparkDeployYarnClusterRunnerTest {

	@Test
	public void submit() throws Exception {
		String appName = "JavaYarnClusterSubmit";
		String schema = "/logplanet/deployment/DE1559267912/ENT8243";
		String brokers = "192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092";
		String zkHosts = "192.168.10.82:2181,192.168.10.83:2181,192.168.10.84:2181";
		String hdfsUrl = "hdfs://obz-hadoop-ha/user/ecube/log-planet";
		
		SparkDeployYarnClusterRunner runner = new SparkDeployYarnClusterRunner();
		runner.submit(appName, schema, brokers, zkHosts, hdfsUrl, "SP");
	}
}
