package com.obzen.pilot.spark.common.data;

//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
//import org.json.simple.parser.ParseException;

import java.io.Serializable;

/**
 * Created by hanmin on 16. 2. 1.
 */
public class EventSub implements Serializable {

    private String event_id;
    private String event_name;
    private String event_url;

    public String getEvent_id() {
        return event_id;
    }

    public String getEvent_name() {
        return event_name;
    }

    public String getEvent_url() {
        return event_url;
    }

    public void setEvent_id(String event_id) {
        this.event_id = event_id;
    }

    public void setEvent_name(String event_name) {
        this.event_name = event_name;
    }

    public void setEvent_url(String event_url) {
        this.event_url = event_url;
    }
}
