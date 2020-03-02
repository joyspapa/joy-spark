package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class CustSearchword implements Serializable {
    private String base_dt;
    private String cust_id;
    private String cookie_id;
    private String searchword;
    private int search_cnt = 0;
    private int search_fail_cnt = 0;

    // Full constructor
    public CustSearchword(
            String base_dt,
            String cust_id,
            String cookie_id,
            String searchword,
            int search_cnt,
            int search_fail_cnt
    ) {
        this.base_dt = base_dt;
        this.cust_id = cust_id;
        this.cookie_id = cookie_id;
        this.searchword = searchword;
        this.search_cnt = search_cnt;
        this.search_fail_cnt = search_fail_cnt;
    }

    public CustSearchword(
            String base_dt,
            String cust_id,
            String cookie_id,
            String searchword
    ) {
        this.base_dt = base_dt;
        this.cust_id = cust_id;
        this.cookie_id = cookie_id;
        this.searchword = searchword;
    }

    public String getBase_dt() {
        return base_dt;
    }

    public String getCust_id() {
        return cust_id;
    }

    public String getCookie_id() {
        return cookie_id;
    }

    public String getSearchword() {
        return searchword;
    }

    public int getSearch_cnt() {
        return search_cnt;
    }

    public void setSearch_cnt(int search_cnt) {
        this.search_cnt = search_cnt;
    }

    public int getSearch_fail_cnt() {
        return search_fail_cnt;
    }

    // sum count
    public void addSearch_cnt(int search_cnt) {
        this.search_cnt = this.search_cnt + search_cnt;
    }

    public void addSearch_fail_cnt(int search_fail_cnt) {
        this.search_fail_cnt = this.search_fail_cnt + search_fail_cnt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CustSearchword [ base_dt=" + base_dt);
        sb.append(", cust_id=" + cust_id);
        sb.append(", cookie_id=" + cookie_id);
        sb.append(", searchword=" + searchword);
        sb.append(", search_fail_cnt=" + search_fail_cnt);
        sb.append("]");
        return sb.toString();
    }
}
