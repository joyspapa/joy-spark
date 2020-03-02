package com.obzen.pilot.spark.common.data;

import java.io.Serializable;

/**
 * Created by hanmin on 16. 2. 1.
 */
public class GroupSub implements Serializable {

    private String groupCity;
    private String groupCountry;
    private String groupName;

    private CategorySub category;

    public String getGroupCity() {
        return groupCity;
    }

    public String getGroupCountry() {
        return groupCountry;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupCity(String groupCity) {
        this.groupCity = groupCity;
    }

    public void setGroupCountry(String groupCountry) {
        this.groupCountry = groupCountry;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public CategorySub getCategory() {
        //return (category != null)? category:new CategorySub();
        return category;
    }

    public void setCategory(CategorySub category) {
        this.category = category;
    }
}
