package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class ContentOrderSummary implements Serializable {
    private String base_dt;
    private String product_cd;
    private String product_nm;
    private String product_catg_l;
    private String product_catg_m;
    private String product_catg_s;
    private String product_catg_l_nm;
    private String product_catg_m_nm;
    private String product_catg_s_nm;
    private int order_visit_cnt;

    public ContentOrderSummary(
            String base_dt,
            String product_cd,
            String product_nm,
            String product_catg_l,
            String product_catg_m,
            String product_catg_s,
            String product_catg_l_nm,
            String product_catg_m_nm,
            String product_catg_s_nm,
            int order_visit_cnt
    ) {
        this.base_dt = base_dt;
        this.product_cd = product_cd;
        this.product_nm = product_nm;
        this.product_catg_l = product_catg_l;
        this.product_catg_m = product_catg_m;
        this.product_catg_s = product_catg_s;
        this.product_catg_l_nm = product_catg_l_nm;
        this.product_catg_m_nm = product_catg_m_nm;
        this.product_catg_s_nm = product_catg_s_nm;
        this.order_visit_cnt = order_visit_cnt;
    }

    public String getBase_dt() {
        return base_dt;
    }

    public void setBase_dt(String base_dt) {
        this.base_dt = base_dt;
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

    public int getOrder_visit_cnt() {
        return order_visit_cnt;
    }

    public void setOrder_visit_cnt(int order_visit_cnt) {
        this.order_visit_cnt = order_visit_cnt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ContentOrderSummary [base_dt=" + base_dt);
        sb.append(", product_cd=" + product_cd);
        sb.append(", product_nm=" + product_nm);
        sb.append(", product_catg_l=" + product_catg_l);
        sb.append(", product_catg_m=" + product_catg_m);
        sb.append(", product_catg_s=" + product_catg_s);
        sb.append(", product_catg_l_nm=" + product_catg_l_nm);
        sb.append(", product_catg_m_nm=" + product_catg_m_nm);
        sb.append(", product_catg_s_nm=" + product_catg_s_nm);
        sb.append(", order_visit_cnt=" + order_visit_cnt);
        sb.append("]");
        return sb.toString();
    }
}
