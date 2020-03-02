package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class VisitDetail implements Serializable {
    private String base_dt;
    private String session_id;
    private String uuid;
    private String cust_id;
    private String cookie_id;
    private String start_time;
    private String end_time;
    private String sex;
    private String age;
    private String place;
    private String cust_grade;
    private String device_type;
    private String app_web_type;
    private String ip_addr;
    private String inbound_chnl_src;
    private String inbound_chnl_medium;
    private String inbound_chnl_campaign;
    private String inbound_chnl_keyword;
    private String new_visit_yn = "N";
    private String regist_visit_yn = "N";
    private String start_page_uri;
    private String end_page_uri;
    private int pageview_cnt = 0;
    private int page_cnt = 0;
    private long stay_time = 0L;
    private int order_cnt = 0;
    private long amount = 0L;

    // Empty constuctor
    public VisitDetail() {
        super();
    }

    public VisitDetail(
            String base_dt,
            String session_id,
            String uuid,
            String cust_id,
            String cookie_id,
            String sex,
            String age,
            String place,
            String cust_grade,
            String device_type,
            String app_web_type,
            String ip_addr,
            String inbound_chnl_src,
            String inbound_chnl_medium,
            String inbound_chnl_campaign,
            String inbound_chnl_keyword
    ) {
        this.base_dt = base_dt;
        this.session_id = session_id;
        this.uuid = uuid;
        this.cust_id = cust_id;
        this.cookie_id = cookie_id;
        this.sex = sex;
        this.age = age;
        this.place = place;
        this.cust_grade = cust_grade;
        this.device_type = device_type;
        this.app_web_type = app_web_type;
        this.ip_addr = ip_addr;
        this.inbound_chnl_src = inbound_chnl_src;
        this.inbound_chnl_medium = inbound_chnl_medium;
        this.inbound_chnl_campaign = inbound_chnl_campaign;
        this.inbound_chnl_keyword = inbound_chnl_keyword;
    }

    public String getBase_dt() {
        return base_dt;
    }

    public void setBase_dt(String base_dt) {
        this.base_dt = base_dt;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
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

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
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

    public String getIp_addr() {
        return ip_addr;
    }

    public void setIp_addr(String ip_addr) {
        this.ip_addr = ip_addr;
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

    public String getNew_visit_yn() {
        return new_visit_yn;
    }

    public void setNew_visit_yn(String new_visit_yn) {
        this.new_visit_yn = new_visit_yn;
    }

    public String getRegist_visit_yn() {
        return regist_visit_yn;
    }

    public void setRegist_visit_yn(String regist_visit_yn) {
        this.regist_visit_yn = regist_visit_yn;
    }

    public String getStart_page_uri() {
        return start_page_uri;
    }

    public void setStart_page_uri(String start_page_uri) {
        this.start_page_uri = start_page_uri;
    }

    public String getEnd_page_uri() {
        return end_page_uri;
    }

    public void setEnd_page_uri(String end_page_uri) {
        this.end_page_uri = end_page_uri;
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

    public void addStay_time(long stay_time) {
        this.stay_time = this.stay_time + stay_time;
    }

    public int getOrder_cnt() {
        return order_cnt;
    }

    public void setOrder_cnt(int order_cnt) {
        this.order_cnt = order_cnt;
    }

    public void addOrder_cnt(int order_cnt) {
        this.order_cnt = this.order_cnt + order_cnt;
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

    public void addPageview_cnt(int pageview_cnt) {
        this.pageview_cnt = this.pageview_cnt + pageview_cnt;
    }

    public void addPage_cnt(int page_cnt) {
        this.page_cnt = this.page_cnt + page_cnt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VisitDetail [ base_dt=" + base_dt);
        sb.append(", session_id=" + session_id);
        sb.append(", uuid=" + uuid);
        sb.append(", cust_id=" + cust_id);
        sb.append(", cookie_id=" + cookie_id);
        sb.append(", start_time=" + start_time);
        sb.append(", end_time=" + end_time);
        sb.append(", sex=" + sex);
        sb.append(", age=" + age);
        sb.append(", place=" + place);
        sb.append(", cust_grade=" + cust_grade);
        sb.append(", device_type=" + device_type);
        sb.append(", app_web_type=" + app_web_type);
        sb.append(", ip_addr=" + ip_addr);
        sb.append(", inbound_chnl_src=" + inbound_chnl_src);
        sb.append(", inbound_chnl_medium=" + inbound_chnl_medium);
        sb.append(", inbound_chnl_campaign=" + inbound_chnl_campaign);
        sb.append(", inbound_chnl_keyword=" + inbound_chnl_keyword);
        sb.append(", new_visit_yn=" + new_visit_yn);
        sb.append(", regist_visit_yn=" + regist_visit_yn);
        sb.append(", start_page_uri=" + start_page_uri);
        sb.append(", end_page_uri=" + end_page_uri);
        sb.append(", pageview_cnt=" + pageview_cnt);
        sb.append(", page_cnt=" + page_cnt);
        sb.append(", stay_time=" + stay_time);
        sb.append(", order_cnt=" + order_cnt);
        sb.append(", amount=" + amount);
        sb.append("]");
        return sb.toString();
    }
}
