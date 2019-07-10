package com.joy.spark.streaming.properties;

import org.apache.spark.streaming.Duration;

public class SparkProperty {

	private String sparkAppName = "DefaultSparkApplName";
	private Duration batchIntervalMilis = new Duration(2 * 1000);
	private String maxRatePerPartition = "1000"; // kafka partition * maxRatePerPartition
	private String sparkDriverCores = "1";
	private String sparkDriverMemory = "1";
	private String sparkDriverMaxResultSize = "1g";
	private String sparkExecutorInstances = "1";
	private String sparkExecutorCores = "1";
	private String sparkExecutorMemory = "1g";
	private String sparkExecutorHeartbeatInterval = "20s";

	public String getSparkAppName() {
		return sparkAppName;
	}

	public void setSparkAppName(String sparkAppName) {
		this.sparkAppName = sparkAppName;
	}

	public Duration getBatchIntervalMilis() {
		return batchIntervalMilis;
	}

	public void setBatchIntervalMilis(Duration batchIntervalMilis) {
		this.batchIntervalMilis = batchIntervalMilis;
	}

	public String getMaxRatePerPartition() {
		return maxRatePerPartition;
	}

	public void setMaxRatePerPartition(String maxRatePerPartition) {
		this.maxRatePerPartition = maxRatePerPartition;
	}

	public String getSparkDriverCores() {
		return sparkDriverCores;
	}

	public void setSparkDriverCores(String sparkDriverCores) {
		this.sparkDriverCores = sparkDriverCores;
	}

	public String getSparkDriverMemory() {
		return sparkDriverMemory;
	}

	public void setSparkDriverMemory(String sparkDriverMemory) {
		this.sparkDriverMemory = sparkDriverMemory;
	}

	public String getSparkDriverMaxResultSize() {
		return sparkDriverMaxResultSize;
	}

	public void setSparkDriverMaxResultSize(String sparkDriverMaxResultSize) {
		this.sparkDriverMaxResultSize = sparkDriverMaxResultSize;
	}

	public String getSparkExecutorInstances() {
		return sparkExecutorInstances;
	}

	public void setSparkExecutorInstances(String sparkExecutorInstances) {
		this.sparkExecutorInstances = sparkExecutorInstances;
	}

	public String getSparkExecutorCores() {
		return sparkExecutorCores;
	}

	public void setSparkExecutorCores(String sparkExecutorCores) {
		this.sparkExecutorCores = sparkExecutorCores;
	}

	public String getSparkExecutorMemory() {
		return sparkExecutorMemory;
	}

	public void setSparkExecutorMemory(String sparkExecutorMemory) {
		this.sparkExecutorMemory = sparkExecutorMemory;
	}

	public String getSparkExecutorHeartbeatInterval() {
		return sparkExecutorHeartbeatInterval;
	}

	public void setSparkExecutorHeartbeatInterval(String sparkExecutorHeartbeatInterval) {
		this.sparkExecutorHeartbeatInterval = sparkExecutorHeartbeatInterval;
	}
}
