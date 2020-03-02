package com.obzen.pilot.spark.common.util;


import java.io.Serializable;

import com.obzen.pilot.spark.common.data.EventInfo;
import com.obzen.pilot.spark.common.data.RsvpsInfo;

/**
 * http://examples.javacodegeeks.com/core-java/json/java-json-parser-example/
 * JSONArray evevtArray= (JSONArray) json.get("event");
 *
 * Created by hanmin on 16. 2. 11.
 */
public class JsonParserUtil implements Serializable {
    //private transient JSONParser parser;
    //private transient JSONObject json;

    private EventInfo eventInfo;
    private RsvpsInfo rsvpsInfo;

    public JsonParserUtil() {
//        if(parser == null) {
//            parser = new JSONParser();
//        }
    }

    public EventInfo parserEventInfo(String oneRecord) {
        eventInfo = new EventInfo();
        /*
        JSONObject groupJsonObj;
        try {
            Object obj = parser.parse(oneRecord);
            json = (JSONObject)obj; //JSONObject => Map

            eventInfo.setEventUrl((String) json.get("event_url"));
            eventInfo.setEventId((String) json.get("id"));
            eventInfo.setEventName((String) json.get("name"));
            JSONObject venueJsonObj =  (JSONObject) json.get("venue");

            // 회원에게만 공개하는 경우 NUll, (This location is shown only to members , http://www.meetup.com/Startup-50K/events/228771913/)
            VenueInfo venueInfo = new VenueInfo();
            if(venueJsonObj != null) {

                venueInfo.setCountry( (String) venueJsonObj.get("country"));
                venueInfo.setCity( (String) venueJsonObj.get("city"));

            } else {

                groupJsonObj = (JSONObject) json.get("group");
                if (groupJsonObj != null) {
                    venueInfo.setCountry((String) groupJsonObj.get("country"));
                    venueInfo.setCity((String) groupJsonObj.get("city"));
                } else {
                    System.out.println("country is NULL, event_id="+eventInfo.getEventId());
                }
            }
            eventInfo.setVenueInfo(venueInfo);

            groupJsonObj = (JSONObject) json.get("group");
            JSONObject categoryJsonObj =  (JSONObject) groupJsonObj.get("category");

            CategorySub category = new CategorySub();
            if(categoryJsonObj != null) {
                category.setName( (String) categoryJsonObj.get("name"));
                category.setShortname( (String) categoryJsonObj.get("shortname"));
            }

            eventInfo.getGroupSub().setCategory(category);

        } catch(ParseException pe) {
            //logger.error("Error: "+ pe.printStackTrace());
            System.err.println("[JsonParserUtil.parserEventInfo] ParseException Error: " + pe.getMessage() + ", jsonLine :" + oneRecord);
        } catch (Exception _ex) {
            System.err.println("[JsonParserUtil.parserEventInfo] Exception Error: " + _ex.getMessage() + ", jsonLine :" + oneRecord);
        } finally {
            if(eventInfo.getVenueInfo() == null ) {
                eventInfo.setVenueInfo(new VenueInfo());
            }
            if(eventInfo.getGroupSub().getCategory() == null ) {
                eventInfo.getGroupSub().setCategory(new CategorySub());
            }
        }
	*/
        return eventInfo;
    }

    public RsvpsInfo parserRsvpsInfo(String oneRecord) {
        rsvpsInfo = new RsvpsInfo();
        /*
        try {
            Object obj = parser.parse(oneRecord);
            json = (JSONObject)obj; //JSONObject => Map

            rsvpsInfo.setResponse((String) json.get("response"));
            JSONObject eventStructure =  (JSONObject) json.get("event");

            EventInfo event = new EventInfo();
            if(eventStructure != null) {
                event.setEventId((String)eventStructure.get("event_id"));
                event.setEventName((String)eventStructure.get("event_name"));
            }

            rsvpsInfo.setEvent(event);
        } catch(ParseException pe) {
            //logger.error("Error: "+ pe.printStackTrace());
            System.err.println("[JsonParserUtil.parserRsvpsInfo] ParseException Error: " + pe.getMessage() + ", jsonLine :" + oneRecord);
        } catch (Exception _ex) {
            System.err.println("[JsonParserUtil.parserRsvpsInfo] Exception Error: " + _ex.getMessage() + ", jsonLine :" + oneRecord);
        } finally {
            if(rsvpsInfo.getEvent() == null ) {
                rsvpsInfo.setEvent(new EventInfo());
            }
        }
	*/
        return rsvpsInfo;
    }

}
