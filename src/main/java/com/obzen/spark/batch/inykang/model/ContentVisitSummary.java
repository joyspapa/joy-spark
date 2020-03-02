package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class ContentVisitSummary implements Serializable {
    private String base_dt;
    private String page_uri;
    private String product_cd;
    private String product_nm;
    private String product_catg_l;
    private String product_catg_m;
    private String product_catg_s;
    private String product_catg_l_nm;
    private String product_catg_m_nm;
    private String product_catg_s_nm;
    private int visit_cnt;
    private int visitor_cnt;
    private int new_visitor_cnt;
    private int pageview_cnt;
    private long stay_time;
    private int pagein_visit_cnt;
    private int pageout_visit_cnt;
    private int bounce_cnt;

    // Full constructor
    public ContentVisitSummary(
            String base_dt,
            String page_uri,
            String product_cd,
            String product_nm,
            String product_catg_l,
            String product_catg_m,
            String product_catg_s,
            String product_catg_l_nm,
            String product_catg_m_nm,
            String product_catg_s_nm,
            int visit_cnt,
            int visitor_cnt,
            int new_visitor_cnt,
            int pageview_cnt,
            long stay_time,
            int pagein_visit_cnt,
            int pageout_visit_cnt,
            int bounce_cnt
    ) {
        this.base_dt = base_dt;
        this.page_uri = page_uri;
        this.product_cd = product_cd;
        this.product_nm = product_nm;
        this.product_catg_l = product_catg_l;
        this.product_catg_m = product_catg_m;
        this.product_catg_s = product_catg_s;
        this.product_catg_l_nm = product_catg_l_nm;
        this.product_catg_m_nm = product_catg_m_nm;
        this.product_catg_s_nm = product_catg_s_nm;
        this.visit_cnt = visit_cnt;
        this.visitor_cnt = visitor_cnt;
        this.new_visitor_cnt = new_visitor_cnt;
        this.pageview_cnt = pageview_cnt;
        this.stay_time = stay_time;
        this.pagein_visit_cnt = pagein_visit_cnt;
        this.pageout_visit_cnt = pageout_visit_cnt;
        this.bounce_cnt = bounce_cnt;
    }

    public ContentVisitSummary(
            String base_dt,
            String page_uri,
            String product_catg_l,
            String product_catg_m,
            String product_catg_s,
            String product_catg_l_nm,
            String product_catg_m_nm,
            String product_catg_s_nm
    ) {
        this.base_dt = base_dt;
        this.page_uri = page_uri;
        this.product_catg_l = product_catg_l;
        this.product_catg_m = product_catg_m;
        this.product_catg_s = product_catg_s;
        this.product_catg_l_nm = product_catg_l_nm;
        this.product_catg_m_nm = product_catg_m_nm;
        this.product_catg_s_nm = product_catg_s_nm;
    }

    public String getBase_dt() {
        return base_dt;
    }

    public void setBase_dt(String base_dt) {
        this.base_dt = base_dt;
    }

    public String getPage_uri() {
        return page_uri;
    }

    public void setPage_uri(String page_uri) {
        this.page_uri = page_uri;
    }


    public String getProduct_cd() {
        return product_cd;
    }

    public void setProduct_cd(String product_cd) {
        this.product_cd = product_cd;
    }

    public String getProduct_nm() {
        return product_nm;
    }

    public void setProduct_nm(String product_nm) {
        this.product_nm = product_nm;
    }

    public String getProduct_catg_l() {
        return product_catg_l;
    }

    public void setProduct_catg_l(String product_catg_l) {
        this.product_catg_l = product_catg_l;
    }

    public String getProduct_catg_m() {
        return product_catg_m;
    }

    public void setProduct_catg_m(String product_catg_m) {
        this.product_catg_m = product_catg_m;
    }

    public String getProduct_catg_s() {
        return product_catg_s;
    }

    public void setProduct_catg_s(String product_catg_s) {
        this.product_catg_s = product_catg_s;
    }

    public String getProduct_catg_l_nm() {
        return product_catg_l_nm;
    }

    public void setProduct_catg_l_nm(String product_catg_l_nm) {
        this.product_catg_l_nm = product_catg_l_nm;
    }

    public String getProduct_catg_m_nm() {
        return product_catg_m_nm;
    }

    public void setProduct_catg_m_nm(String product_catg_m_nm) {
        this.product_catg_m_nm = product_catg_m_nm;
    }

    public String getProduct_catg_s_nm() {
        return product_catg_s_nm;
    }

    public void setProduct_catg_s_nm(String product_catg_s_nm) {
        this.product_catg_s_nm = product_catg_s_nm;
    }

    public int getVisit_cnt() {
        return visit_cnt;
    }

    public void setVisit_cnt(int visit_cnt) {
        this.visit_cnt = visit_cnt;
    }

    public int getVisitor_cnt() {
        return visitor_cnt;
    }

    public void setVisitor_cnt(int visitor_cnt) {
        this.visitor_cnt = visitor_cnt;
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

    public long getStay_time() {
        return stay_time;
    }

    public void setStay_time(long stay_time) {
        this.stay_time = stay_time;
    }

    public int getPagein_visit_cnt() {
        return pagein_visit_cnt;
    }

    public void setPagein_visit_cnt(int pagein_visit_cnt) {
        this.pagein_visit_cnt = pagein_visit_cnt;
    }

    public int getPageout_visit_cnt() {
        return pageout_visit_cnt;
    }

    public void setPageout_visit_cnt(int pageout_visit_cnt) {
        this.pageout_visit_cnt = pageout_visit_cnt;
    }

    public int getBounce_cnt() {
        return bounce_cnt;
    }

    public void setBounce_cnt(int bounce_cnt) {
        this.bounce_cnt = bounce_cnt;
    }

    public void addVisit_cnt(int visit_cnt) {
        this.visit_cnt = this.visit_cnt + visit_cnt;
    }

    public void addVisitor_cnt(int visitor_cnt) {
        this.visitor_cnt = this.visitor_cnt + visitor_cnt;
    }

    public void addNew_visitor_cnt(int new_visitor_cnt) {
        this.new_visitor_cnt = this.new_visitor_cnt + new_visitor_cnt;
    }

    public void addPageview_cnt(int pageview_cnt) {
        this.pageview_cnt = this.pageview_cnt + pageview_cnt;
    }

    public void addStay_time(long stay_time) {
        this.stay_time = this.stay_time + stay_time;
    }

    public void addPagein_visit_cnt(int pagein_visit_cnt) {
        this.pagein_visit_cnt = this.pagein_visit_cnt + pagein_visit_cnt;
    }

    public void addPageout_visit_cnt(int pageout_visit_cnt) {
        this.pageout_visit_cnt = this.pageout_visit_cnt + pageout_visit_cnt;
    }

    public void addBounce_cnt(int bounce_cnt) {
        this.bounce_cnt = this.bounce_cnt + bounce_cnt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ContentVisitSummary [ base_dt=" + base_dt);
        sb.append(", page_uri=" + page_uri);
        sb.append(", product_catg_l=" + product_catg_l);
        sb.append(", product_catg_m=" + product_catg_m);
        sb.append(", product_catg_s=" + product_catg_s);
        sb.append(", product_catg_l_nm=" + product_catg_l_nm);
        sb.append(", product_catg_m_nm=" + product_catg_m_nm);
        sb.append(", product_catg_s_nm=" + product_catg_s_nm);
        sb.append(", visit_cnt=" + visit_cnt);
        sb.append(", visitor_cnt=" + visitor_cnt);
        sb.append(", new_visitor_cnt=" + new_visitor_cnt);
        sb.append(", pageview_cnt=" + pageview_cnt);
        sb.append(", stay_time=" + stay_time);
        sb.append(", pagein_visit_cnt=" + pagein_visit_cnt);
        sb.append(", pageout_visit_cnt=" + pageout_visit_cnt);
        sb.append(", bounce_cnt=" + bounce_cnt);
        sb.append("]");
        return sb.toString();
    }
}
