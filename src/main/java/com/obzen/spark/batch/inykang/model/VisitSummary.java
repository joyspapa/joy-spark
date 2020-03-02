package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class VisitSummary implements Serializable {
    private String base_dt;
    private String sex;
    private String age;
    private String place;
    private String cust_grade;
    private String device_type;
    private String app_web_type;
    private String inbound_chnl_src;
    private String inbound_chnl_medium;
    private String inbound_chnl_campaign;
    private String inbound_chnl_keyword;
    private int visit_cnt = 0;
    //private int visitor_cnt = 0;
    //private int cust_cnt = 0;
    private int new_visitor_cnt = 0;
    private int pageview_cnt = 0;
    private int page_cnt = 0;
    private long stay_time = 0L;
    private int bounce_cnt = 0;
    private int order_visit_cnt = 0;
    private long amount = 0L;

    private Set<String> customerKeySet = new HashSet<>();
    private Set<String> custIdSet = new HashSet<>();

    // Empty constructor
    public VisitSummary() {
        super();
    }

    // Full constructor
    public VisitSummary(
            String base_dt,
            String sex,
            String age,
            String place,
            String cust_grade,
            String device_type,
            String app_web_type,
            String inbound_chnl_src,
            String inbound_chnl_medium,
            String inbound_chnl_campaign,
            String inbound_chnl_keyword,
            int visit_cnt,
            int new_visitor_cnt,
            int pageview_cnt,
            int page_cnt,
            long stay_time,
            int bounce_cnt,
            int order_visit_cnt,
            long amount
    ) {
        this.base_dt = base_dt;
        this.sex = sex;
        this.age = age;
        this.place = place;
        this.cust_grade = cust_grade;
        this.device_type = device_type;
        this.app_web_type = app_web_type;
        this.inbound_chnl_src = inbound_chnl_src;
        this.inbound_chnl_medium = inbound_chnl_medium;
        this.inbound_chnl_campaign = inbound_chnl_campaign;
        this.inbound_chnl_keyword = inbound_chnl_keyword;
        this.visit_cnt = visit_cnt;
        this.new_visitor_cnt = new_visitor_cnt;
        this.pageview_cnt = pageview_cnt;
        this.page_cnt = page_cnt;
        this.stay_time = stay_time;
        this.bounce_cnt = bounce_cnt;
        this.order_visit_cnt = order_visit_cnt;
        this.amount = amount;
    }

    public VisitSummary(
            String base_dt,
            String sex,
            String age,
            String place,
            String cust_grade
    ) {
        this.base_dt = base_dt;
        this.sex = sex;
        this.age = age;
        this.place = place;
        this.cust_grade = cust_grade;
    }

    public String getBase_dt() {
        return base_dt;
    }

    public void setBase_dt(String base_dt) {
        this.base_dt = base_dt;
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

    public String getDevice_type() {
        return device_type;
    }

    public void setDevice_type(String device_type) {
        this.device_type = device_type;
    }

    public String getApp_web_type() {
        return app_web_type;
    }

    public void setApp_web_type(String app_web_type) {
        this.app_web_type = app_web_type;
    }

    public String getInbound_chnl_src() {
        return inbound_chnl_src;
    }

    public void setInbound_chnl_src(String inbound_chnl_src) {
        this.inbound_chnl_src = inbound_chnl_src;
    }

    public String getInbound_chnl_medium() {
        return inbound_chnl_medium;
    }

    public void setInbound_chnl_medium(String inbound_chnl_medium) {
        this.inbound_chnl_medium = inbound_chnl_medium;
    }

    public String getInbound_chnl_campaign() {
        return inbound_chnl_campaign;
    }

    public void setInbound_chnl_campaign(String inbound_chnl_campaign) {
        this.inbound_chnl_campaign = inbound_chnl_campaign;
    }

    public String getInbound_chnl_keyword() {
        return inbound_chnl_keyword;
    }

    public void setInbound_chnl_keyword(String inbound_chnl_keyword) {
        this.inbound_chnl_keyword = inbound_chnl_keyword;
    }

    public int getVisit_cnt() {
        return visit_cnt;
    }

    public void setVisit_cnt(int visit_cnt) {
        this.visit_cnt = visit_cnt;
    }

    public int getNew_visitor_cnt() {
        return new_visitor_cnt;
    }

    public void setNew_visitor_cnt(int new_visitor_cnt) {
        this.new_visitor_cnt = new_visitor_cnt;
    }

    public int getPageview_cnt() {
        return pageview_cnt;
    }

    public void setPageview_cnt(int pageview_cnt) {
        this.pageview_cnt = pageview_cnt;
    }

    public int getPage_cnt() {
        return page_cnt;
    }

    public void setPage_cnt(int page_cnt) {
        this.page_cnt = page_cnt;
    }

    public long getStay_time() {
        return stay_time;
    }

    public void setStay_time(long stay_time) {
        this.stay_time = stay_time;
    }

    public int getBounce_cnt() {
        return bounce_cnt;
    }

    public void setBounce_cnt(int bounce_cnt) {
        this.bounce_cnt = bounce_cnt;
    }

    public int getOrder_visit_cnt() {
        return order_visit_cnt;
    }

    public void setOrder_visit_cnt(int order_visit_cnt) {
        this.order_visit_cnt = order_visit_cnt;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public Set<String> getCustomerKeySet() {
        return customerKeySet;
    }

    public void setCustomerKeySet(Set<String> customerKeySet) {
        this.customerKeySet = customerKeySet;
    }

    public Set<String> getCustIdSet() {
        return custIdSet;
    }

    public void setCustIdSet(Set<String> custIdSet) {
        this.custIdSet = custIdSet;
    }

    //Sum count funcs
    public void addVisit_cnt(int visit_cnt) {
        this.visit_cnt = this.visit_cnt + visit_cnt;
    }

    public void addNew_visitor_cnt(int new_visitor_cnt) {
        this.new_visitor_cnt = this.new_visitor_cnt + new_visitor_cnt;
    }

    public void addPageview_cnt(int pageview_cnt) {
        this.pageview_cnt = this.pageview_cnt + pageview_cnt;
    }

    public void addPage_cnt(int page_cnt) {
        this.page_cnt = this.page_cnt + page_cnt;
    }

    public void addStay_time(long stay_time) {
        this.stay_time = this.stay_time + stay_time;
    }

    public void addBounce_cnt(int bounce_cnt) {
        this.bounce_cnt = this.bounce_cnt + bounce_cnt;
    }

    public void addOrder_visit_cnt(int order_cnt) {
        this.order_visit_cnt = this.order_visit_cnt + order_cnt;
    }

    public void addAmount(long amount) {
        this.amount = this.amount + amount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VisitSummary [ base_dt=" + base_dt);
        sb.append(", sex=" + sex);
        sb.append(", age=" + age);
        sb.append(", place=" + place);
        sb.append(", cust_grade=" + cust_grade);
        sb.append(", device_type=" + device_type);
        sb.append(", app_web_type=" + app_web_type);
        sb.append(", inbound_chnl_src=" + inbound_chnl_src);
        sb.append(", inbound_chnl_medium=" + inbound_chnl_medium);
        sb.append(", inbound_chnl_campaign=" + inbound_chnl_campaign);
        sb.append(", inbound_chnl_keyword=" + inbound_chnl_keyword);
        sb.append(", visit_cnt=" + visit_cnt);
        sb.append(", new_visitor_cnt=" + new_visitor_cnt);
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
