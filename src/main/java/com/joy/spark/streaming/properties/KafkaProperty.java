package com.joy.spark.streaming.properties;

import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaProperty {

	private String brokers = "localhost:9092";
	private String topics = "DEFAULT-IN-TOPIC";
	private Class<?> keyDeserializer = StringDeserializer.class;
	private Class<?> valueDeserializer = StringDeserializer.class;

	private boolean enableAutoCommit = false;
	private AutoOffsetResetType autoOffsetReset = AutoOffsetResetType.EARLIEST; // earliest | latest
	private String groupId = "group-id-default-spark";
	private String clientId = "client-id-default-spark";

	public String getBrokers() {
		return brokers;
	}

	public void setBrokers(String brokers) {
		this.brokers = brokers;
	}

	public String getTopics() {
		return topics;
	}

	public void setTopics(String topics) {
		this.topics = topics;
	}

	public Class<?> getKeyDeserializer() {
		return keyDeserializer;
	}

	public void setKeyDeserializer(Class<?> keyDeserializer) {
		this.keyDeserializer = keyDeserializer;
	}

	public Class<?> getValueDeserializer() {
		return valueDeserializer;
	}

	public void setValueDeserializer(Class<?> valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
	}

	public boolean isEnableAutoCommit() {
		return enableAutoCommit;
	}

	public void setEnableAutoCommit(boolean enableAutoCommit) {
		this.enableAutoCommit = enableAutoCommit;
	}

	public String getAutoOffsetReset() {
		return autoOffsetReset.getName();
	}

	public void setAutoOffsetReset(AutoOffsetResetType autoOffsetReset) {
		this.autoOffsetReset = autoOffsetReset;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

}
