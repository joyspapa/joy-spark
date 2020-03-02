package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class CustVisitDaily implements Serializable {
    private String base_dt;
    private String cust_id;
    private String cookie_id;
    private String sex;
    private String age;
    private String place;
    private String cust_grade;
    private int visit_cnt = 0;
    private int pageview_cnt = 0;
    private int page_cnt = 0;
    private long stay_time = 0L;
    private int bounce_cnt = 0;
    private int order_visit_cnt = 0;
    private long amount = 0L;

    public CustVisitDaily(
            String base_dt,
            String cust_id,
            String cookie_id,
            String sex,
            String age,
            String place,
            String cust_grade
    ) {
        this.base_dt = base_dt;
        this.cust_id = cust_id;
        this.cookie_id = cookie_id;
        this.sex = sex;
        this.age = age;
        this.place = place;
        this.cust_grade = cust_grade;
    }

    // Full constructor
    public CustVisitDaily(
            String base_dt,
            String cust_id,
            String cookie_id,
            String sex,
            String age,
            String place,
            String cust_grade,
            int visit_cnt,
            int pageview_cnt,
            int page_cnt,
            long stay_time,
            int bounce_cnt,
            int order_visit_cnt,
            long amount
    ) {
        this.base_dt = base_dt;
        this.cust_id = cust_id;
        this.cookie_id = cookie_id;
        this.sex = sex;
        this.age = age;
        this.place = place;
        this.cust_grade = cust_grade;
        this.visit_cnt = visit_cnt;
        this.pageview_cnt = pageview_cnt;
        this.page_cnt = page_cnt;
        this.stay_time = stay_time;
        this.bounce_cnt = bounce_cnt;
        this.order_visit_cnt = order_visit_cnt;
        this.amount = amount;
    }

    public String getBase_dt() {
        return base_dt;
    }

    public void setBase_dt(String base_dt) {
        this.base_dt = base_dt;
    }

    public String getCust_id() {
        return cust_id;
    }

    public void setCust_id(String cust_id) {
        this.cust_id = cust_id;
    }

    public String getCookie_id() {
        return cookie_id;
    }

    public void setCookie_id(String cookie_id) {
        this.cookie_id = cookie_id;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getPlace() {
        return place;
    }

    public void setPlace(String place) {
        this.place = place;
    }

    public String getCust_grade() {
        return cust_grade;
    }

    public void setCust_grade(String cust_grade) {
        this.cust_grade = cust_grade;
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

    public int getPage_cnt() {
        return page_cnt;
    }

    public void setPage_cnt(int page_cnt) {
        this.page_cnt = page_cnt;
    }

    public void addPage_cnt(int page_cnt) {
        this.page_cnt = this.page_cnt + page_cnt;
    }

    public long getStay_time() {
        return stay_time;
    }

    public void setStay_time(long stay_time) {
        this.stay_time = stay_time;
    }

    public void addStay_time(long stay_time) {
        this.stay_time = this.stay_time + stay_time;
    }

    public int getBounce_cnt() {
        return bounce_cnt;
    }

    public void setBounce_cnt(int bounce_cnt) {
        this.bounce_cnt = bounce_cnt;
    }

    public void addBounce_cnt(int bounce_cnt) {
        this.bounce_cnt = this.bounce_cnt + bounce_cnt;
    }

    public int getOrder_visit_cnt() {
        return order_visit_cnt;
    }

    public void setOrder_visit_cnt(int order_cnt) {
        this.order_visit_cnt = order_cnt;
    }

    public void addOrder_visit_cnt(int order_cnt) {
        this.order_visit_cnt = this.order_visit_cnt + order_cnt;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public void addAmount(long amount) {
        this.amount = this.amount + amount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CustVisitDaily [ base_dt=" + base_dt);
        sb.append(", cust_id=" + cust_id);
        sb.append(", cookie_id=" + cookie_id);
        sb.append(", sex=" + sex);
        sb.append(", age=" + age);
        sb.append(", place=" + place);
        sb.append(", cust_grade=" + cust_grade);
        sb.append(", visit_cnt=" + visit_cnt);
        sb.append(", pageview_cnt=" + pageview_cnt);
        sb.append(", page_cnt=" + page_cnt);
        sb.append(", stay_time=" + stay_time);
        sb.append(", bounce_cnt=" + bounce_cnt);
        sb.append(", order_visit_cnt=" + order_visit_cnt);
        sb.append(", amount=" + amount);
        sb.append("]");
        return sb.toString();
    }
}
