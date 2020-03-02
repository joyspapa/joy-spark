package com.obzen.spark.batch.inykang.model;

public class HourVisitSummary {
    private String base_dt;
    private String base_hr;
    private String dayofweek;
    private int visit_cnt;
    private int pageview_cnt;

    // Full constructor
    public HourVisitSummary(
            String base_dt,
            String base_hr,
            String dayofweek,
            int visit_cnt,
            int pageview_cnt
    ) {
        this.base_dt = base_dt;
        this.base_hr = base_hr;
        this.dayofweek = dayofweek;
        this.visit_cnt = visit_cnt;
        this.pageview_cnt = pageview_cnt;
    }

    public String getBase_dt() {
        return base_dt;
    }

    public void setBase_dt(String base_dt) {
        this.base_dt = base_dt;
    }

    public String getBase_hr() {
        return base_hr;
    }

    public void setBase_hr(String base_hr) {
        this.base_hr = base_hr;
    }

    public String getDayofweek() {
        return dayofweek;
    }

    public void setDayofweek(String dayofweek) {
        this.dayofweek = dayofweek;
    }

    public int getVisit_cnt() {
        return visit_cnt;
    }

    public void setVisit_cnt(int visit_cnt) {
        this.visit_cnt = visit_cnt;
    }

    public void addVisit_cnt(int visit_cnt) {
        this.visit_cnt = this.visit_cnt + visit_cnt;
    }

    public int getPageview_cnt() {
        return pageview_cnt;
    }

    public void setPageview_cnt(int pageview_cnt) {
        this.pageview_cnt = pageview_cnt;
    }

    public void addPageview_cnt(int pageview_cnt) {
        this.pageview_cnt = this.pageview_cnt + pageview_cnt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HourVisitSummary [ base_dt=" + base_dt);
        sb.append(", base_hr=" + base_hr);
        sb.append(", dayofweek=" + dayofweek);
        sb.append(", visit_cnt=" + visit_cnt);
        sb.append(", pageview_cnt=" + pageview_cnt);
        sb.append("]");
        return sb.toString();
    }

}
