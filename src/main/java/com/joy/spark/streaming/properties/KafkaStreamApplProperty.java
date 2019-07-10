package com.joy.spark.streaming.properties;

public class KafkaStreamApplProperty {

	private SparkProperty sparkProp = new SparkProperty();
	private KafkaProperty kafkaProp = new KafkaProperty();

	private String schemaId = "DefaultSchemaId";
	private String zkHosts = "DefaultZkHosts";
	private String hdfsUrl = "DefaultHdfsUrl";

	public SparkProperty getSparkProp() {
		return sparkProp;
	}

	public void setSparkProp(SparkProperty sparkProp) {
		this.sparkProp = sparkProp;
	}

	public KafkaProperty getKafkaProp() {
		return kafkaProp;
	}

	public void setKafkaProp(KafkaProperty kafkaProp) {
		this.kafkaProp = kafkaProp;
	}

	public String getSchemaId() {
		return schemaId;
	}

	public void setSchemaId(String schemaId) {
		this.schemaId = schemaId;
	}

	public String getZkHosts() {
		return zkHosts;
	}

	public void setZkHosts(String zkHosts) {
		this.zkHosts = zkHosts;
	}

	public String getHdfsUrl() {
		return hdfsUrl;
	}

	public void setHdfsUrl(String hdfsUrl) {
		this.hdfsUrl = hdfsUrl;
	}

}
