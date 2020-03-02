package com.obzen.pilot.spark.common.data;

import java.io.Serializable;

/**
 * Created by hanmin on 16. 2. 1.
 */
public class VenueInfo implements Serializable {
//    private static Logger logger = LoggerFactory.getLogger(VenueInfo.class);
    //private JSONParser parser; //= new JSONParser();
    //private JSONObject json ;
    //
    private String zip;
    private String country;
    private String city;
    private String address_1;
    private String name;
    private long id;
    private long mtime;


    public VenueInfo() {
        //parser = new JSONParser();
    }

    public VenueInfo(String jsonLine) {
        this();
        /*
        Object obj = null;
        try {
            obj = parser.parse(jsonLine);
            json = (JSONObject)obj; //JSONObject => Map

            zip = (String) json.get("zip");
            country = (String) json.get("country");
            city = (String) json.get("city");
            address_1 = (String) json.get("address_1");
            name = (String) json.get("name");
            id = (Long) json.get("id");
            mtime = (Long) json.get("mtime");

        } catch(ParseException pe) {
//            logger.error("Error: {}", pe.getMessage());
            System.out.println("Error: " + pe.getMessage() + ", jsonLine :" + jsonLine);
        }
        */
    }


    public String getZip() {
        return zip;
    }

    public String getCountry() {
        return country;
    }

    public String getCity() {
        return city;
    }

    public String getAddress_1() {
        return address_1;
    }

    public String getName() {
        return name;
    }

    public long getId() {
        return id;
    }

    public long getMtime() {
        return mtime;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setAddress_1(String address_1) {
        this.address_1 = address_1;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setMtime(long mtime) {
        this.mtime = mtime;
    }
}
