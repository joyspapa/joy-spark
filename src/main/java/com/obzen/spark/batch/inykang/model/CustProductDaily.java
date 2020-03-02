package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class CustProductDaily implements Serializable {
    private String base_dt;
    private String cust_id;
    private String cookie_id;
    private String product_cd;
    private String product_nm;
    private String product_catg_l;
    private String product_catg_m;
    private String product_catg_s;
    private String product_catg_l_nm;
    private String product_catg_m_nm;
    private String product_catg_s_nm;
    private String product_brand_cd;
    private String product_brand_nm;
    private int prod_detail_pageview_cnt;
    private long prod_detail_page_stay_time;
    private int basket_click_cnt;
    private int zzim_click_cnt;

    public CustProductDaily(
            String base_dt,
            String cust_id,
            String cookie_id,
            String product_cd,
            String product_nm,
            String product_catg_l,
            String product_catg_m,
            String product_catg_s,
            String product_brand_cd,
            String product_catg_l_nm,
            String product_catg_m_nm,
            String product_catg_s_nm,
            String product_brand_nm,
            int prod_detail_pageview_cnt,
            long prod_detail_page_stay_time,
            int basket_click_cnt,
            int zzim_click_cnt
    ) {
        this.base_dt = base_dt;
        this.cust_id = cust_id;
        this.cookie_id = cookie_id;
        this.product_cd = product_cd;
        this.product_nm = product_nm;
        this.product_catg_l = product_catg_l;
        this.product_catg_m = product_catg_m;
        this.product_catg_s = product_catg_s;
        this.product_brand_cd = product_brand_cd;
        this.product_catg_l_nm = product_catg_l_nm;
        this.product_catg_m_nm = product_catg_m_nm;
        this.product_catg_s_nm = product_catg_s_nm;
        this.product_brand_nm = product_brand_nm;
        this.prod_detail_pageview_cnt = prod_detail_pageview_cnt;
        this.prod_detail_page_stay_time = prod_detail_page_stay_time;
        this.basket_click_cnt = basket_click_cnt;
        this.zzim_click_cnt = zzim_click_cnt;
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

    public String getProduct_brand_cd() {
        return product_brand_cd;
    }

    public void setProduct_brand_cd(String product_brand_cd) {
        this.product_brand_cd = product_brand_cd;
    }

    public String getProduct_brand_nm() {
        return product_brand_nm;
    }

    public void setProduct_brand_nm(String product_brand_nm) {
        this.product_brand_nm = product_brand_nm;
    }

    public int getProd_detail_pageview_cnt() {
        return prod_detail_pageview_cnt;
    }

    public void setProd_detail_pageview_cnt(int prod_detail_pageview_cnt) {
        this.prod_detail_pageview_cnt = prod_detail_pageview_cnt;
    }

    public void addProd_detail_pageview_cnt(int prod_detail_pageview_cnt) {
        this.prod_detail_pageview_cnt = this.prod_detail_pageview_cnt + prod_detail_pageview_cnt;
    }

    public long getProd_detail_page_stay_time() {
        return prod_detail_page_stay_time;
    }

    public void setProd_detail_page_stay_time(long prod_detail_page_stay_time) {
        this.prod_detail_page_stay_time = prod_detail_page_stay_time;
    }

    public void addProd_detail_page_stay_time(long prod_detail_page_stay_time) {
        this.prod_detail_page_stay_time = this.prod_detail_page_stay_time + prod_detail_page_stay_time;
    }

    public int getBasket_click_cnt() {
        return basket_click_cnt;
    }

    public void setBasket_click_cnt(int basket_click_cnt) {
        this.basket_click_cnt = basket_click_cnt;
    }

    public void addBasket_click_cnt(int basket_click_cnt) {
        this.basket_click_cnt = this.basket_click_cnt + basket_click_cnt;
    }

    public int getZzim_click_cnt() {
        return zzim_click_cnt;
    }

    public void setZzim_click_cnt(int zzim_click_cnt) {
        this.zzim_click_cnt = zzim_click_cnt;
    }

    public void addZzim_click_cnt(int zzim_click_cnt) {
        this.zzim_click_cnt = this.zzim_click_cnt + zzim_click_cnt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CustProductDaily [ base_dt=" + base_dt);
        sb.append(", cust_id=" + cust_id);
        sb.append(", cookie_id=" + cookie_id);
        sb.append(", product_cd=" + product_cd);
        sb.append(", product_nm=" + product_nm);
        sb.append(", product_catg_l=" + product_catg_l);
        sb.append(", product_catg_m=" + product_catg_m);
        sb.append(", product_catg_s=" + product_catg_s);
        sb.append(", product_catg_l_nm=" + product_catg_l_nm);
        sb.append(", product_catg_m_nm=" + product_catg_m_nm);
        sb.append(", product_catg_s_nm=" + product_catg_s_nm);
        sb.append(", product_brand_cd=" + product_brand_cd);
        sb.append(", product_brand_nm=" + product_brand_nm);
        sb.append(", prod_detail_pageview_cnt=" + prod_detail_pageview_cnt);
        sb.append(", prod_detail_page_stay_time=" + prod_detail_page_stay_time);
        sb.append(", basket_click_cnt=" + basket_click_cnt);
        sb.append(", zzim_click_cnt=" + zzim_click_cnt);
        sb.append("]");
        return sb.toString();
    }
}
