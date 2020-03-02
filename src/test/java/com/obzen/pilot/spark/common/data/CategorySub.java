package com.obzen.pilot.spark.common.data;

import java.io.Serializable;

/**
 * Created by hanmin on 16. 2. 1.
 */
public class CategorySub implements Serializable {

    private String name;
    private String shortname;
    private String id;

    public String getName() {
        return name;
    }

    public String getShortname() {
        return shortname;
    }

    public String getId() {
        return id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setShortname(String shortname) {
        this.shortname = shortname;
    }

    public void setId(String id) {
        this.id = id;
    }
}
