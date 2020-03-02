package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class CustProductOrder implements Serializable {
    private String base_dt;
    private String cust_id;
    private String cookie_id;
    private String product_cd;
    private String product_nm;
    private String product_catg_l;
    private String product_catg_m;
    private String product_catg_s;
    private String product_brand_cd;
    private String product_catg_l_nm;
    private String product_catg_m_nm;
    private String product_catg_s_nm;
    private String product_brand_nm;
    private int order_cnt;
    private long amount;

    // Full constructor
    public CustProductOrder(
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
            int order_cnt,
            long amount
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
        this.order_cnt = order_cnt;
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

    public String getProduct_brand_cd() {
        return product_brand_cd;
    }

    public void setProduct_brand_cd(String product_brand_cd) {
        this.product_brand_cd = product_brand_cd;
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

    public String getProduct_brand_nm() {
        return product_brand_nm;
    }

    public void setProduct_brand_nm(String product_brand_nm) {
        this.product_brand_nm = product_brand_nm;
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CustProductOrder [base_dt=" + base_dt);
        sb.append(", cust_id=" + cust_id);
        sb.append(", cookie_id=" + cookie_id);
        sb.append(", product_cd=" + product_cd);
        sb.append(", product_nm=" + product_nm);
        sb.append(", product_catg_l=" + product_catg_l);
        sb.append(", product_catg_m=" + product_catg_m);
        sb.append(", product_catg_s=" + product_catg_s);
        sb.append(", product_brand_cd=" + product_brand_cd);
        sb.append(", product_catg_l_nm=" + product_catg_l_nm);
        sb.append(", product_catg_m_nm=" + product_catg_m_nm);
        sb.append(", product_catg_s_nm=" + product_catg_s_nm);
        sb.append(", product_brand_nm=" + product_brand_nm);
        sb.append(", order_cnt=" + order_cnt);
        sb.append(", amount=" + amount);
        sb.append("]");
        return sb.toString();
    }
}
