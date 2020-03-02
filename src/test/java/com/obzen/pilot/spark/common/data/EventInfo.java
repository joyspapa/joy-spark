
package com.obzen.pilot.spark.common.data;

import java.io.Serializable;

/**
 *  
 *   OpenEvents
 *
 */
public class EventInfo implements Serializable {
	//protected final Logger logger = Logger.getLogger(EventInfo.class);

	private String eventUrl;
	private String eventId;
	private long mtime;
	private String eventName;
	private String status;
	private long yesRsvpCount;
	private String photoUrl;
	private String paymentRequired;

	private VenueInfo venueInfo;
	private GroupSub groupSub;

	public EventInfo() {
	}

	public String getEventUrl() {
		return eventUrl;
	}

	public String getEventId() {
		return eventId;
	}

	public long getMtime() {
		return mtime;
	}

	public String getEventName() {
		StringBuilder sb = new StringBuilder(100).append("\"").append(eventName).append("\"");
		return sb.toString();
	}

	public String getStatus() {
		return status;
	}

	public long getYesRsvpCount() {
		return yesRsvpCount;
	}

	public String getPhotoUrl() {
		return photoUrl;
	}

	public String getPaymentRequired() {
		return paymentRequired;
	}


	public void setEventUrl(String eventUrl) {
		this.eventUrl = eventUrl;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public void setMtime(long mtime) {
		this.mtime = mtime;
	}

	public void setEventName(String name) {
		this.eventName = name;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public void setYesRsvpCount(long yesRsvpCount) {
		this.yesRsvpCount = yesRsvpCount;
	}

	public void setPhotoUrl(String photo_url) {
		this.photoUrl = photo_url;
	}

	public void setPaymentRequired(String payment_required) {
		this.paymentRequired = payment_required;
	}

	public VenueInfo getVenueInfo() {
		return venueInfo;
	}

	public void setVenueInfo(VenueInfo venueInfo) {
		this.venueInfo = venueInfo;
	}

	public GroupSub getGroupSub() {
		if (groupSub == null) {
			setGroupSub(new GroupSub());
		}
		return groupSub;
	}

	public void setGroupSub(GroupSub groupSub) {
		this.groupSub = groupSub;
	}
}
