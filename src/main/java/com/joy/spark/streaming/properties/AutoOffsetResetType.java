package com.joy.spark.streaming.properties;

public enum AutoOffsetResetType {
	EARLIEST   ("earliest"), 
	LATEST     ("latest");
	
	private String name;
	
	AutoOffsetResetType(String name) {
		this.name = name;
	}
	
	public String getName() {
		return this.name;
	}
}
