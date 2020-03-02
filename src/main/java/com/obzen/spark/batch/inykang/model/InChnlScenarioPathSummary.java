package com.obzen.spark.batch.inykang.model;

public class InChnlScenarioPathSummary {
    private String base_dt;
    private String scenario_nm;
    private String scenario_page_seq;
    private String pv_type;
    private String inbound_chnl_src;
    private String inbound_chnl_medium;
    private String inbound_chnl_campaign;
    private int visit_cnt;

    // Full constructor
    public InChnlScenarioPathSummary(
            String base_dt,
            String scenario_nm,
            String scenario_page_seq,
            String pv_type,
            String inbound_chnl_src,
            String inbound_chnl_medium,
            String inbound_chnl_campaign,
            int visit_cnt
    ) {
        this.base_dt = base_dt;
        this.scenario_nm = scenario_nm;
        this.scenario_page_seq = scenario_page_seq;
        this.pv_type = pv_type;
        this.inbound_chnl_src = inbound_chnl_src;
        this.inbound_chnl_medium = inbound_chnl_medium;
        this.inbound_chnl_campaign = inbound_chnl_campaign;
        this.visit_cnt = visit_cnt;
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

    public int getVisit_cnt() {
        return visit_cnt;
    }

    public void addVisit_cnt(int visit_cnt) {
        this.visit_cnt = this.visit_cnt + visit_cnt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("InChnlScenarioPathVisit [ base_dt=" + base_dt);
        sb.append(", scenario_nm=" + scenario_nm);
        sb.append(", scenario_page_seq=" + scenario_page_seq);
        sb.append(", pv_type=" + pv_type);
        sb.append(", inbound_chnl_src=" + inbound_chnl_src);
        sb.append(", inbound_chnl_medium=" + inbound_chnl_medium);
        sb.append(", inbound_chnl_campaign=" + inbound_chnl_campaign);
        sb.append(", visit_cnt=" + visit_cnt);
        sb.append("]");
        return sb.toString();
    }
}
