package com.obzen.pilot.spark.common.data;

import java.io.Serializable;

/**
 * Created by hanmin on 16. 2. 1.
 */
public class RsvpsInfo implements Serializable {

    private EventInfo event;

    private String response;
    private String member;


    public RsvpsInfo() {
    }

    public String getMember() {
        return member;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public void setMember(String member) {
        this.member = member;
    }

    public EventInfo getEvent() {
        return event;
    }

    public void setEvent(EventInfo event) {
        this.event = event;
    }
}
