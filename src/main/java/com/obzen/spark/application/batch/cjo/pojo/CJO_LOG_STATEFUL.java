/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.obzen.spark.application.batch.cjo.pojo;

import java.io.*;
import java.text.SimpleDateFormat;

/**
 * CJO_LOG_BASE definition.
 * <p>
 * Code generated by Apache Ignite Schema Import utility: 11/01/2017.
 */
public class CJO_LOG_STATEFUL implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    
    private int visitCnt;
    
    public int getVisitCnt() {
		return visitCnt;
	}

	public void setVisitCnt(int visitCnt) {
		this.visitCnt = visitCnt;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}

	private long eventTime;
    
    /**
     * Value for log_time.
     */
    private String log_time;

    /**
     * Value for cust_age.
     */
    private String cust_age;

    /**
     * Value for app_cd.
     */
    private String app_cd;

    /**
     * Value for brand_cd.
     */
    private String brand_cd;

    /**
     * Value for chn_cd.
     */
    private String chn_cd;

    /**
     * Value for click_cd.
     */
    private String click_cd;

    /**
     * Value for cur_url_chn_tp.
     */
    private String cur_url_chn_tp;

    /**
     * Value for cur_pg_info1.
     */
    private String cur_pg_info1;

    /**
     * Value for cur_pg_info2.
     */
    private String cur_pg_info2;

    /**
     * Value for date_wday.
     */
    private String date_wday;

    /**
     * Value for cust_empl_fl.
     */
    private String cust_empl_fl;

    /**
     * Value for cust_grade.
     */
    private String cust_grade;

    /**
     * Value for infl_cd.
     */
    private String infl_cd;

    /**
     * Value for infl_grp_cd.
     */
    private String infl_grp_cd;

    /**
     * Value for intl_pgm_cd.
     */
    private String intl_pgm_cd;

    /**
     * Value for md_cd.
     */
    private String md_cd;

    /**
     * Value for udid.
     */
    private String udid;

    /**
     * Value for pgm_cd.
     */
    private String pgm_cd;

    /**
     * Value for hometab_pic.
     */
    private String hometab_pic;

    /**
     * Value for ref_pg_info1.
     */
    private String ref_pg_info1;

    /**
     * Value for ref_pg_info2.
     */
    private String ref_pg_info2;

    /**
     * Value for search_result.
     */
    private String search_result;

    /**
     * Value for search_word.
     */
    private String search_word;

    /**
     * Value for cust_gender.
     */
    private String cust_gender;

    /**
     * Value for shop_cd.
     */
    private String shop_cd;

    /**
     * Value for sid.
     */
    private String sid;

    /**
     * Value for std_cate_cd.
     */
    private String std_cate_cd;

    /**
     * Value for uid.
     */
    private String uid;

    /**
     * Value for cust_cd.
     */
    private String cust_cd;

    /**
     * Value for cust_id.
     */
    private String cust_id;

    /**
     * Value for cust_visit_fl.
     */
    private String cust_visit_fl;

    /**
     * Value for visit_login_fl.
     */
    private String visit_login_fl;

    /**
     * Value for visit_pg_seq.
     */
    private String visit_pg_seq;

    /**
     * Value for vipmall_clk_yn.
     */
    private String vipmall_clk_yn = "N";

    /**
     * Value for vipclub_clk_yn.
     */
    private String vipclub_clk_yn = "N";

    /**
     * Empty constructor.
     */
    public CJO_LOG_STATEFUL() {
        // No-op.
    }

    /**
     * Full constructor.
     */
    public CJO_LOG_STATEFUL(
            String log_time,
            String cust_age,
            String app_cd,
            String brand_cd,
            String chn_cd,
            String click_cd,
            String cur_url_chn_tp,
            String cur_pg_info1,
            String cur_pg_info2,
            String date_wday,
            String cust_empl_fl,
            String cust_grade,
            String infl_cd,
            String infl_grp_cd,
            String intl_pgm_cd,
            String md_cd,
            String udid,
            String pgm_cd,
            String hometab_pic,
            String ref_pg_info1,
            String ref_pg_info2,
            String search_result,
            String search_word,
            String cust_gender,
            String shop_cd,
            String sid,
            String std_cate_cd,
            String uid,
            String cust_cd,
            String cust_id,
            String cust_visit_fl,
            String visit_login_fl,
            String visit_pg_seq,
            String vipmall_clk_yn,
            String vipclub_clk_yn
    ) {
    	try {
    		this.eventTime = dateFormat.parse(log_time).getTime();
    	} catch (Exception ex) {
    		System.err.print("parsing Error : " + ex.toString() + " , log_time=" + log_time + " , cust_cd=" + cust_cd);;
    	}
    	
        this.log_time = log_time;
        this.cust_age = cust_age;
        this.app_cd = app_cd;
        this.brand_cd = brand_cd;
        this.chn_cd = chn_cd;
        this.click_cd = click_cd;
        this.cur_url_chn_tp = cur_url_chn_tp;
        this.cur_pg_info1 = cur_pg_info1;
        this.cur_pg_info2 = cur_pg_info2;
        this.date_wday = date_wday;
        this.cust_empl_fl = cust_empl_fl;
        this.cust_grade = cust_grade;
        this.infl_cd = infl_cd;
        this.infl_grp_cd = infl_grp_cd;
        this.intl_pgm_cd = intl_pgm_cd;
        this.md_cd = md_cd;
        this.udid = udid;
        this.pgm_cd = pgm_cd;
        this.hometab_pic = hometab_pic;
        this.ref_pg_info1 = ref_pg_info1;
        this.ref_pg_info2 = ref_pg_info2;
        this.search_result = search_result;
        this.search_word = search_word;
        this.cust_gender = cust_gender;
        this.shop_cd = shop_cd;
        this.sid = sid;
        this.std_cate_cd = std_cate_cd;
        this.uid = uid;
        this.cust_cd = cust_cd;
        this.cust_id = cust_id;
        this.cust_visit_fl = cust_visit_fl;
        this.visit_login_fl = visit_login_fl;
        this.visit_pg_seq = visit_pg_seq;
        this.vipmall_clk_yn = vipmall_clk_yn;
        this.vipclub_clk_yn = vipclub_clk_yn;
        this.pgm_cd = pgm_cd;
    }

    /**
     * Core constructor.
     */
    public CJO_LOG_STATEFUL(
            CJO_LOG_CORE core
    ) {
        this.log_time = core.getLog_time();
        this.app_cd = core.getApp_cd();
        this.brand_cd = core.getBrand_cd();
        this.chn_cd = core.getChn_cd();
        this.click_cd = core.getClick_cd();
        this.cur_url_chn_tp = core.getCur_url_chn_tp();
        this.cur_pg_info1 = core.getCur_pg_info1();
        this.cur_pg_info2 = core.getCur_pg_info2();
        this.date_wday = core.getDate_wday();
        this.infl_cd = core.getInfl_cd();
        this.infl_grp_cd = core.getInfl_grp_cd();
        this.intl_pgm_cd = core.getIntl_pgm_cd();
        this.md_cd = core.getMd_cd();
        this.pgm_cd = core.getPgm_cd();
        this.hometab_pic = core.getHometab_pic();
        this.ref_pg_info1 = core.getRef_pg_info1();
        this.ref_pg_info2 = core.getRef_pg_info2();
        this.search_result = core.getSearch_result();
        this.search_word = core.getSearch_word();
        this.cust_gender = core.getCust_gender();
        this.sid = core.getSid();
        this.std_cate_cd = core.getStd_cate_cd();
        this.uid = core.getUid();
        this.cust_cd = core.getCust_cd();
        this.cust_id = core.getCust_id();
        this.cust_visit_fl = core.getCust_visit_fl();
        this.visit_login_fl = core.getVisit_login_fl();
        this.visit_pg_seq = core.getVisit_pg_seq();
    }

    /**
     * Gets log_time.
     *
     * @return Value for log_time.
     */
    public String getLog_time() {
        return log_time;
    }

    /**
     * Sets log_time.
     *
     * @param log_time New value for log_time.
     */
    public void setLog_time(String log_time) {
        this.log_time = log_time;
    }

    /**
     * Gets cust_age.
     *
     * @return Value for cust_age.
     */
    public String getCust_age() {
        return cust_age;
    }

    /**
     * Sets cust_age.
     *
     * @param cust_age New value for cust_age.
     */
    public void setCust_age(String cust_age) {
        this.cust_age = cust_age;
    }

    /**
     * Gets app_cd.
     *
     * @return Value for app_cd.
     */
    public String getApp_cd() {
        return app_cd;
    }

    /**
     * Sets app_cd.
     *
     * @param app_cd New value for app_cd.
     */
    public void setApp_cd(String app_cd) {
        this.app_cd = app_cd;
    }

    /**
     * Gets brand_cd.
     *
     * @return Value for brand_cd.
     */
    public String getBrand_cd() {
        return brand_cd;
    }

    /**
     * Sets brand_cd.
     *
     * @param brand_cd New value for brand_cd.
     */
    public void setBrand_cd(String brand_cd) {
        this.brand_cd = brand_cd;
    }

    /**
     * Gets chn_cd.
     *
     * @return Value for chn_cd.
     */
    public String getChn_cd() {
        return chn_cd;
    }

    /**
     * Sets chn_cd.
     *
     * @param chn_cd New value for chn_cd.
     */
    public void setChn_cd(String chn_cd) {
        this.chn_cd = chn_cd;
    }

    /**
     * Gets click_cd.
     *
     * @return Value for click_cd.
     */
    public String getClick_cd() {
        return click_cd;
    }

    /**
     * Sets click_cd.
     *
     * @param click_cd New value for click_cd.
     */
    public void setClick_cd(String click_cd) {
        this.click_cd = click_cd;
    }

    /**
     * Gets cur_url_chn_tp.
     *
     * @return Value for cur_url_chn_tp.
     */
    public String getCur_url_chn_tp() {
        return cur_url_chn_tp;
    }

    /**
     * Sets cur_url_chn_tp.
     *
     * @param cur_url_chn_tp New value for cur_url_chn_tp.
     */
    public void setCur_url_chn_tp(String cur_url_chn_tp) {
        this.cur_url_chn_tp = cur_url_chn_tp;
    }

    /**
     * Gets cur_pg_info1.
     *
     * @return Value for cur_pg_info1.
     */
    public String getCur_pg_info1() {
        return cur_pg_info1;
    }

    /**
     * Sets cur_pg_info1.
     *
     * @param cur_pg_info1 New value for cur_pg_info1.
     */
    public void setCur_pg_info1(String cur_pg_info1) {
        this.cur_pg_info1 = cur_pg_info1;
    }

    /**
     * Gets cur_pg_info2.
     *
     * @return Value for cur_pg_info2.
     */
    public String getCur_pg_info2() {
        return cur_pg_info2;
    }

    /**
     * Sets cur_pg_info2.
     *
     * @param cur_pg_info2 New value for cur_pg_info2.
     */
    public void setCur_pg_info2(String cur_pg_info2) {
        this.cur_pg_info2 = cur_pg_info2;
    }

    /**
     * Gets date_wday.
     *
     * @return Value for date_wday.
     */
    public String getDate_wday() {
        return date_wday;
    }

    /**
     * Sets date_wday.
     *
     * @param date_wday New value for date_wday.
     */
    public void setDate_wday(String date_wday) {
        this.date_wday = date_wday;
    }

    /**
     * Gets cust_empl_fl.
     *
     * @return Value for cust_empl_fl.
     */
    public String getCust_empl_fl() {
        return cust_empl_fl;
    }

    /**
     * Sets cust_empl_fl.
     *
     * @param cust_empl_fl New value for cust_empl_fl.
     */
    public void setCust_empl_fl(String cust_empl_fl) {
        this.cust_empl_fl = cust_empl_fl;
    }

    /**
     * Gets cust_grade.
     *
     * @return Value for cust_grade.
     */
    public String getCust_grade() {
        return cust_grade;
    }

    /**
     * Sets cust_grade.
     *
     * @param cust_grade New value for cust_grade.
     */
    public void setCust_grade(String cust_grade) {
        this.cust_grade = cust_grade;
    }

    /**
     * Gets infl_cd.
     *
     * @return Value for infl_cd.
     */
    public String getInfl_cd() {
        return infl_cd;
    }

    /**
     * Sets infl_cd.
     *
     * @param infl_cd New value for infl_cd.
     */
    public void setInfl_cd(String infl_cd) {
        this.infl_cd = infl_cd;
    }

    /**
     * Gets infl_grp_cd.
     *
     * @return Value for infl_grp_cd.
     */
    public String getInfl_grp_cd() {
        return infl_grp_cd;
    }

    /**
     * Sets infl_grp_cd.
     *
     * @param infl_grp_cd New value for infl_grp_cd.
     */
    public void setInfl_grp_cd(String infl_grp_cd) {
        this.infl_grp_cd = infl_grp_cd;
    }

    /**
     * Gets intl_pgm_cd.
     *
     * @return Value for intl_pgm_cd.
     */
    public String getIntl_pgm_cd() {
        return intl_pgm_cd;
    }

    /**
     * Sets intl_pgm_cd.
     *
     * @param intl_pgm_cd New value for intl_pgm_cd.
     */
    public void setIntl_pgm_cd(String intl_pgm_cd) {
        this.intl_pgm_cd = intl_pgm_cd;
    }

    /**
     * Gets md_cd.
     *
     * @return Value for md_cd.
     */
    public String getMd_cd() {
        return md_cd;
    }

    /**
     * Sets md_cd.
     *
     * @param md_cd New value for md_cd.
     */
    public void setMd_cd(String md_cd) {
        this.md_cd = md_cd;
    }

    /**
     * Gets udid.
     *
     * @return Value for udid.
     */
    public String getUdid() {
        return udid;
    }

    /**
     * Sets udid.
     *
     * @param udid New value for udid.
     */
    public void setUdid(String udid) {
        this.udid = udid;
    }

    /**
     * Gets pgm_cd.
     *
     * @return Value for pgm_cd.
     */
    public String getPgm_cd() {
        return pgm_cd;
    }

    /**
     * Sets pgm_cd.
     *
     * @param pgm_cd New value for pgm_cd.
     */
    public void setPgm_cd(String pgm_cd) {
        this.pgm_cd = pgm_cd;
    }

    /**
     * Gets hometab_pic.
     *
     * @return Value for hometab_pic.
     */
    public String getHometab_pic() {
        return hometab_pic;
    }

    /**
     * Sets hometab_pic.
     *
     * @param hometab_pic New value for hometab_pic.
     */
    public void setHometab_pic(String hometab_pic) {
        this.hometab_pic = hometab_pic;
    }

    /**
     * Gets ref_pg_info1.
     *
     * @return Value for ref_pg_info1.
     */
    public String getRef_pg_info1() {
        return ref_pg_info1;
    }

    /**
     * Sets ref_pg_info1.
     *
     * @param ref_pg_info1 New value for ref_pg_info1.
     */
    public void setRef_pg_info1(String ref_pg_info1) {
        this.ref_pg_info1 = ref_pg_info1;
    }

    /**
     * Gets ref_pg_info2.
     *
     * @return Value for ref_pg_info2.
     */
    public String getRef_pg_info2() {
        return ref_pg_info2;
    }

    /**
     * Sets ref_pg_info2.
     *
     * @param ref_pg_info2 New value for ref_pg_info2.
     */
    public void setRef_pg_info2(String ref_pg_info2) {
        this.ref_pg_info2 = ref_pg_info2;
    }

    /**
     * Gets search_result.
     *
     * @return Value for search_result.
     */
    public String getSearch_result() {
        return search_result;
    }

    /**
     * Sets search_result.
     *
     * @param search_result New value for search_result.
     */
    public void setSearch_result(String search_result) {
        this.search_result = search_result;
    }

    /**
     * Gets search_word.
     *
     * @return Value for search_word.
     */
    public String getSearch_word() {
        return search_word;
    }

    /**
     * Sets search_word.
     *
     * @param search_word New value for search_word.
     */
    public void setSearch_word(String search_word) {
        this.search_word = search_word;
    }

    /**
     * Gets cust_gender.
     *
     * @return Value for cust_gender.
     */
    public String getCust_gender() {
        return cust_gender;
    }

    /**
     * Sets cust_gender.
     *
     * @param cust_gender New value for cust_gender.
     */
    public void setCust_gender(String cust_gender) {
        this.cust_gender = cust_gender;
    }

    /**
     * Gets shop_cd.
     *
     * @return Value for shop_cd.
     */
    public String getShop_cd() {
        return shop_cd;
    }

    /**
     * Sets shop_cd.
     *
     * @param shop_cd New value for shop_cd.
     */
    public void setShop_cd(String shop_cd) {
        this.shop_cd = shop_cd;
    }

    /**
     * Gets sid.
     *
     * @return Value for sid.
     */
    public String getSid() {
        return sid;
    }

    /**
     * Sets sid.
     *
     * @param sid New value for sid.
     */
    public void setSid(String sid) {
        this.sid = sid;
    }

    /**
     * Gets std_cate_cd.
     *
     * @return Value for std_cate_cd.
     */
    public String getStd_cate_cd() {
        return std_cate_cd;
    }

    /**
     * Sets std_cate_cd.
     *
     * @param std_cate_cd New value for std_cate_cd.
     */
    public void setStd_cate_cd(String std_cate_cd) {
        this.std_cate_cd = std_cate_cd;
    }

    /**
     * Gets uid.
     *
     * @return Value for uid.
     */
    public String getUid() {
        return uid;
    }

    /**
     * Sets uid.
     *
     * @param uid New value for uid.
     */
    public void setUid(String uid) {
        this.uid = uid;
    }

    /**
     * Gets cust_cd.
     *
     * @return Value for cust_cd.
     */
    public String getCust_cd() {
        return cust_cd;
    }

    /**
     * Sets cust_cd.
     *
     * @param cust_cd New value for cust_cd.
     */
    public void setCust_cd(String cust_cd) {
        this.cust_cd = cust_cd;
    }

    /**
     * Gets cust_id.
     *
     * @return Value for cust_id.
     */
    public String getCust_id() {
        return cust_id;
    }

    /**
     * Sets cust_id.
     *
     * @param cust_id New value for cust_id.
     */
    public void setCust_id(String cust_id) {
        this.cust_id = cust_id;
    }

    /**
     * Gets cust_visit_fl.
     *
     * @return Value for cust_visit_fl.
     */
    public String getCust_visit_fl() {
        return cust_visit_fl;
    }

    /**
     * Sets cust_visit_fl.
     *
     * @param cust_visit_fl New value for cust_visit_fl.
     */
    public void setCust_visit_fl(String cust_visit_fl) {
        this.cust_visit_fl = cust_visit_fl;
    }

    /**
     * Gets visit_login_fl.
     *
     * @return Value for visit_login_fl.
     */
    public String getVisit_login_fl() {
        return visit_login_fl;
    }

    /**
     * Sets visit_login_fl.
     *
     * @param visit_login_fl New value for visit_login_fl.
     */
    public void setVisit_login_fl(String visit_login_fl) {
        this.visit_login_fl = visit_login_fl;
    }

    /**
     * Gets visit_pg_seq.
     *
     * @return Value for visit_pg_seq.
     */
    public String getVisit_pg_seq() {
        return visit_pg_seq;
    }

    /**
     * Sets visit_pg_seq.
     *
     * @param visit_pg_seq New value for visit_pg_seq.
     */
    public void setVisit_pg_seq(String visit_pg_seq) {
        this.visit_pg_seq = visit_pg_seq;
    }

    /**
     * Gets vipmall_clk_yn.
     *
     * @return Value for vipmall_clk_yn.
     */
    public String getVipmall_clk_yn() {
        return vipmall_clk_yn;
    }

    /**
     * Sets vipmall_clk_yn.
     *
     * @param vipmall_clk_yn New value for vipmall_clk_yn.
     */
    public void setVipmall_clk_yn(String vipmall_clk_yn) {
        this.vipmall_clk_yn = vipmall_clk_yn;
    }

    /**
     * Gets vipclub_clk_yn.
     *
     * @return Value for vipclub_clk_yn.
     */
    public String getVipclub_clk_yn() {
        return vipclub_clk_yn;
    }

    /**
     * Sets vipclub_clk_yn.
     *
     * @param vipclub_clk_yn New value for vipclub_clk_yn.
     */
    public void setVipclub_clk_yn(String vipclub_clk_yn) {
        this.vipclub_clk_yn = vipclub_clk_yn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof CJO_LOG_STATEFUL))
            return false;

        CJO_LOG_STATEFUL that = (CJO_LOG_STATEFUL) o;

        if (log_time != null ? !log_time.equals(that.log_time) : that.log_time != null)
            return false;

        if (cust_age != null ? !cust_age.equals(that.cust_age) : that.cust_age != null)
            return false;

        if (app_cd != null ? !app_cd.equals(that.app_cd) : that.app_cd != null)
            return false;

        if (brand_cd != null ? !brand_cd.equals(that.brand_cd) : that.brand_cd != null)
            return false;

        if (chn_cd != null ? !chn_cd.equals(that.chn_cd) : that.chn_cd != null)
            return false;

        if (click_cd != null ? !click_cd.equals(that.click_cd) : that.click_cd != null)
            return false;

        if (cur_url_chn_tp != null ? !cur_url_chn_tp.equals(that.cur_url_chn_tp) : that.cur_url_chn_tp != null)
            return false;

        if (cur_pg_info1 != null ? !cur_pg_info1.equals(that.cur_pg_info1) : that.cur_pg_info1 != null)
            return false;

        if (cur_pg_info2 != null ? !cur_pg_info2.equals(that.cur_pg_info2) : that.cur_pg_info2 != null)
            return false;

        if (date_wday != null ? !date_wday.equals(that.date_wday) : that.date_wday != null)
            return false;

        if (cust_empl_fl != null ? !cust_empl_fl.equals(that.cust_empl_fl) : that.cust_empl_fl != null)
            return false;

        if (cust_grade != null ? !cust_grade.equals(that.cust_grade) : that.cust_grade != null)
            return false;

        if (infl_cd != null ? !infl_cd.equals(that.infl_cd) : that.infl_cd != null)
            return false;

        if (infl_grp_cd != null ? !infl_grp_cd.equals(that.infl_grp_cd) : that.infl_grp_cd != null)
            return false;

        if (intl_pgm_cd != null ? !intl_pgm_cd.equals(that.intl_pgm_cd) : that.intl_pgm_cd != null)
            return false;

        if (md_cd != null ? !md_cd.equals(that.md_cd) : that.md_cd != null)
            return false;

        if (udid != null ? !udid.equals(that.udid) : that.udid != null)
            return false;

        if (pgm_cd != null ? !pgm_cd.equals(that.pgm_cd) : that.pgm_cd != null)
            return false;

        if (hometab_pic != null ? !hometab_pic.equals(that.hometab_pic) : that.hometab_pic != null)
            return false;

        if (ref_pg_info1 != null ? !ref_pg_info1.equals(that.ref_pg_info1) : that.ref_pg_info1 != null)
            return false;

        if (ref_pg_info2 != null ? !ref_pg_info2.equals(that.ref_pg_info2) : that.ref_pg_info2 != null)
            return false;

        if (search_result != null ? !search_result.equals(that.search_result) : that.search_result != null)
            return false;

        if (search_word != null ? !search_word.equals(that.search_word) : that.search_word != null)
            return false;

        if (cust_gender != null ? !cust_gender.equals(that.cust_gender) : that.cust_gender != null)
            return false;

        if (shop_cd != null ? !shop_cd.equals(that.shop_cd) : that.shop_cd != null)
            return false;

        if (sid != null ? !sid.equals(that.sid) : that.sid != null)
            return false;

        if (std_cate_cd != null ? !std_cate_cd.equals(that.std_cate_cd) : that.std_cate_cd != null)
            return false;

        if (uid != null ? !uid.equals(that.uid) : that.uid != null)
            return false;

        if (cust_cd != null ? !cust_cd.equals(that.cust_cd) : that.cust_cd != null)
            return false;

        if (cust_id != null ? !cust_id.equals(that.cust_id) : that.cust_id != null)
            return false;

        if (cust_visit_fl != null ? !cust_visit_fl.equals(that.cust_visit_fl) : that.cust_visit_fl != null)
            return false;

        if (visit_login_fl != null ? !visit_login_fl.equals(that.visit_login_fl) : that.visit_login_fl != null)
            return false;

        if (visit_pg_seq != null ? !visit_pg_seq.equals(that.visit_pg_seq) : that.visit_pg_seq != null)
            return false;

        if (vipmall_clk_yn != null ? !vipmall_clk_yn.equals(that.vipmall_clk_yn) : that.vipmall_clk_yn != null)
            return false;

        if (vipclub_clk_yn != null ? !vipclub_clk_yn.equals(that.vipclub_clk_yn) : that.vipclub_clk_yn != null)
            return false;
        
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int res = log_time != null ? log_time.hashCode() : 0;

        res = 31 * res + (cust_age != null ? cust_age.hashCode() : 0);

        res = 31 * res + (app_cd != null ? app_cd.hashCode() : 0);

        res = 31 * res + (brand_cd != null ? brand_cd.hashCode() : 0);

        res = 31 * res + (chn_cd != null ? chn_cd.hashCode() : 0);

        res = 31 * res + (click_cd != null ? click_cd.hashCode() : 0);

        res = 31 * res + (cur_url_chn_tp != null ? cur_url_chn_tp.hashCode() : 0);

        res = 31 * res + (cur_pg_info1 != null ? cur_pg_info1.hashCode() : 0);

        res = 31 * res + (cur_pg_info2 != null ? cur_pg_info2.hashCode() : 0);

        res = 31 * res + (date_wday != null ? date_wday.hashCode() : 0);

        res = 31 * res + (cust_empl_fl != null ? cust_empl_fl.hashCode() : 0);

        res = 31 * res + (cust_grade != null ? cust_grade.hashCode() : 0);

        res = 31 * res + (infl_cd != null ? infl_cd.hashCode() : 0);

        res = 31 * res + (infl_grp_cd != null ? infl_grp_cd.hashCode() : 0);

        res = 31 * res + (intl_pgm_cd != null ? intl_pgm_cd.hashCode() : 0);

        res = 31 * res + (md_cd != null ? md_cd.hashCode() : 0);

        res = 31 * res + (udid != null ? udid.hashCode() : 0);

        res = 31 * res + (pgm_cd != null ? pgm_cd.hashCode() : 0);

        res = 31 * res + (hometab_pic != null ? hometab_pic.hashCode() : 0);

        res = 31 * res + (ref_pg_info1 != null ? ref_pg_info1.hashCode() : 0);

        res = 31 * res + (ref_pg_info2 != null ? ref_pg_info2.hashCode() : 0);

        res = 31 * res + (search_result != null ? search_result.hashCode() : 0);

        res = 31 * res + (search_word != null ? search_word.hashCode() : 0);

        res = 31 * res + (cust_gender != null ? cust_gender.hashCode() : 0);

        res = 31 * res + (shop_cd != null ? shop_cd.hashCode() : 0);

        res = 31 * res + (sid != null ? sid.hashCode() : 0);

        res = 31 * res + (std_cate_cd != null ? std_cate_cd.hashCode() : 0);

        res = 31 * res + (uid != null ? uid.hashCode() : 0);

        res = 31 * res + (cust_cd != null ? cust_cd.hashCode() : 0);

        res = 31 * res + (cust_id != null ? cust_id.hashCode() : 0);

        res = 31 * res + (cust_visit_fl != null ? cust_visit_fl.hashCode() : 0);

        res = 31 * res + (visit_login_fl != null ? visit_login_fl.hashCode() : 0);

        res = 31 * res + (visit_pg_seq != null ? visit_pg_seq.hashCode() : 0);

        res = 31 * res + (vipmall_clk_yn != null ? vipmall_clk_yn.hashCode() : 0);

        res = 31 * res + (vipclub_clk_yn != null ? vipclub_clk_yn.hashCode() : 0);

        return res; 
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "CJO_LOG_BASE [log_time=" + log_time +
                ", cust_age=" + cust_age +
                ", app_cd=" + app_cd +
                ", brand_cd=" + brand_cd +
                ", chn_cd=" + chn_cd +
                ", click_cd=" + click_cd +
                ", cur_url_chn_tp=" + cur_url_chn_tp +
                ", cur_pg_info1=" + cur_pg_info1 +
                ", cur_pg_info2=" + cur_pg_info2 +
                ", date_wday=" + date_wday +
                ", cust_empl_fl=" + cust_empl_fl +
                ", cust_grade=" + cust_grade +
                ", infl_cd=" + infl_cd +
                ", infl_grp_cd=" + infl_grp_cd +
                ", intl_pgm_cd=" + intl_pgm_cd +
                ", md_cd=" + md_cd +
                ", udid=" + udid +
                ", pgm_cd=" + pgm_cd +
                ", hometab_pic=" + hometab_pic +
                ", ref_pg_info1=" + ref_pg_info1 +
                ", ref_pg_info2=" + ref_pg_info2 +
                ", search_result=" + search_result +
                ", search_word=" + search_word +
                ", cust_gender=" + cust_gender +
                ", shop_cd=" + shop_cd +
                ", sid=" + sid +
                ", std_cate_cd=" + std_cate_cd +
                ", uid=" + uid +
                ", cust_cd=" + cust_cd +
                ", cust_id=" + cust_id +
                ", cust_visit_fl=" + cust_visit_fl +
                ", visit_login_fl=" + visit_login_fl +
                ", visit_pg_seq=" + visit_pg_seq +
                ", vipmall_clk_yn=" + vipmall_clk_yn +
                ", vipclub_clk_yn=" + vipclub_clk_yn +
                "]";
    }


}
