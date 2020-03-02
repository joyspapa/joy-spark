package com.obzen.spark.application.batch.cjo.pojo;

public class CJO_LOG_STATEFUL_SUM {

	String sid;
	int cacheInsertedCount;
	int eventOutputCount;
	
	public String getSid() {
		return sid;
	}
	public void setSid(String sid) {
		this.sid = sid;
	}
	public int getCacheInsertedCount() {
		return cacheInsertedCount;
	}
	public void setCacheInsertedCount(int cacheInsertedCount) {
		this.cacheInsertedCount = cacheInsertedCount;
	}
	public int getEventOutputCount() {
		return eventOutputCount;
	}
	public void setEventOutputCount(int eventOutputCount) {
		this.eventOutputCount = eventOutputCount;
	}
}
