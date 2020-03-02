package com.obzen.spark.batch.inykang.model;

public class InChnlScenarioPathVisit {
    private String base_dt;
    private String scenario_nm;
    private String scenario_page_seq;
    private String pv_type;
    private String session_id;
    private String cust_id;
    private String inbound_chnl_src;
    private String inbound_chnl_medium;
    private String inbound_chnl_campaign;
    private String sex;
    private String age;
    private String device_type;

    public InChnlScenarioPathVisit(
            String base_dt,
            String scenario_nm,
            String scenario_page_seq,
            String pv_type,
            String session_id,
            String cust_id,
            String inbound_chnl_src,
            String inbound_chnl_medium,
            String inbound_chnl_campaign,
            String sex,
            String age,
            String device_type
    ) {
        this.base_dt = base_dt;
        this.scenario_nm = scenario_nm;
        this.scenario_page_seq = scenario_page_seq;
        this.pv_type = pv_type;
        this.session_id = session_id;
        this.cust_id = cust_id;
        this.inbound_chnl_src = inbound_chnl_src;
        this.inbound_chnl_medium = inbound_chnl_medium;
        this.inbound_chnl_campaign = inbound_chnl_campaign;
        this.sex = sex;
        this.age = age;
        this.device_type = device_type;
    }

    public String getBase_dt() {
        return base_dt;
    }

    public void setBase_dt(String base_dt) {
        this.base_dt = base_dt;
    }

    public String getScenario_nm() {
        return scenario_nm;
    }

    public void setScenario_nm(String scenario_nm) {
        this.scenario_nm = scenario_nm;
    }

    public String getScenario_page_seq() {
        return scenario_page_seq;
    }

    public void setScenario_page_seq(String scenario_page_seq) {
        this.scenario_page_seq = scenario_page_seq;
    }

    public String getPv_type() {
        return pv_type;
    }

    public void setPv_type(String pv_type) {
        this.pv_type = pv_type;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public String getCust_id() {
        return cust_id;
    }

    public void setCust_id(String cust_id) {
        this.cust_id = cust_id;
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

    public String getDevice_type() {
        return device_type;
    }

    public void setDevice_type(String device_type) {
        this.device_type = device_type;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("InChnlScenarioPathVisit [ base_dt=" + base_dt);
        sb.append(", scenario_nm=" + scenario_nm);
        sb.append(", scenario_page_seq=" + scenario_page_seq);
        sb.append(", pv_type=" + pv_type);
        sb.append(", session_id=" + session_id);
        sb.append(", cust_id=" + cust_id);
        sb.append(", inbound_chnl_src=" + inbound_chnl_src);
        sb.append(", inbound_chnl_medium=" + inbound_chnl_medium);
        sb.append(", inbound_chnl_campaign=" + inbound_chnl_campaign);
        sb.append(", sex=" + sex);
        sb.append(", age=" + age);
        sb.append(", device_type=" + device_type);
        sb.append("]");
        return sb.toString();
    }
}
