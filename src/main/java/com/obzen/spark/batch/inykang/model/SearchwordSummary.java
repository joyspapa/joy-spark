package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class SearchwordSummary implements Serializable {
    private String base_dt;
    private String searchword;
    private int search_cnt = 0;
    private int search_fail_cnt = 0;

    public SearchwordSummary(
            String base_dt,
            String searchword,
            int search_cnt,
            int search_fail_cnt
    ) {
        this.base_dt = base_dt;
        this.searchword = searchword;
        this.search_cnt = search_cnt;
        this.search_fail_cnt = search_fail_cnt;
    }

    public String getBase_dt() {
        return base_dt;
    }

    public String getSearchword() {
        return searchword;
    }

    public int getSearch_cnt() {
        return search_cnt;
    }

    public void addSearch_cnt(int search_cnt) {
        this.search_cnt = this.search_cnt + search_cnt;
    }

    public int getSearch_fail_cnt() {
        return search_fail_cnt;
    }

    public void addSearch_fail_cnt(int search_fail_cnt) {
        this.search_fail_cnt = this.search_fail_cnt + search_fail_cnt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SearchwordSummary [ base_dt=" + base_dt);
        sb.append(", searchword=" + searchword);
        sb.append(", search_fail_cnt=" + search_fail_cnt);
        sb.append("]");
        return sb.toString();
    }
}
