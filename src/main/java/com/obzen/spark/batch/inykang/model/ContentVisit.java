package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class ContentVisit implements Serializable {
    private String sessionId;
    private String custId;
    private String cookieId;
    private String currPageUri;
    private String productCd;
    private String categoryCdL;
    private String categoryCdM;
    private String categoryCdS;
    private int newVisitCnt;
    private int pageview_cnt;
    private long stayTime;
    private int inboundPageCnt;
    private int outboundPageCnt;
    private int bounceCnt;
    private int basketClickCnt;
    private int zzimClickCnt;

    // Empty constructor
    public ContentVisit(){super();}

    // Full constructor
    public ContentVisit(
            String sessionId,
            String custId,
            String cookieId,
            String currPageUri,
            String productCd,
            String categoryCdL,
            String categoryCdM,
            String categoryCdS,
            int newVisitCnt,
            int pageview_cnt,
            long stayTime,
            int inboundPageCnt,
            int outboundPageCnt,
            int bounceCnt,
            int basketClickCnt,
            int zzimClickCnt
    ) {
        this.sessionId = sessionId;
        this.custId = custId;
        this.cookieId = cookieId;
        this.currPageUri = currPageUri;
        this.productCd = productCd;
        this.categoryCdL = categoryCdL;
        this.categoryCdM = categoryCdM;
        this.categoryCdS = categoryCdS;
        this.newVisitCnt = newVisitCnt;
        this.pageview_cnt = pageview_cnt;
        this.stayTime = stayTime;
        this.inboundPageCnt = inboundPageCnt;
        this.outboundPageCnt = outboundPageCnt;
        this.bounceCnt = bounceCnt;
        this.basketClickCnt = basketClickCnt;
        this.zzimClickCnt = zzimClickCnt;
    }

    public ContentVisit(
            String productCd
            , String currPageUri
            , String custId
            , int newVisitCnt
    ) {
        this.productCd = productCd;
        this.currPageUri = currPageUri;
        this.custId = custId;
        this.newVisitCnt = newVisitCnt;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getCustId() {
        return custId;
    }

    public void setCustId(String custId) {
        this.custId = custId;
    }

    public String getCookieId() {
        return cookieId;
    }

    public void setCookieId(String cookieId) {
        this.cookieId = cookieId;
    }

    public String getCurrPageUri() {
        return currPageUri;
    }

    public void setCurrPageUri(String currPageUri) {
        this.currPageUri = currPageUri;
    }

    public String getProductCd() {
        return productCd;
    }

    public void setProductCd(String productCd) {
        this.productCd = productCd;
    }

    public String getCategoryCdL() {
        return categoryCdL;
    }

    public void setCategoryCdL(String categoryCdL) {
        this.categoryCdL = categoryCdL;
    }

    public String getCategoryCdM() {
        return categoryCdM;
    }

    public void setCategoryCdM(String categoryCdM) {
        this.categoryCdM = categoryCdM;
    }

    public String getCategoryCdS() {
        return categoryCdS;
    }

    public void setCategoryCdS(String categoryCdS) {
        this.categoryCdS = categoryCdS;
    }

    public int getNewVisitCnt() {
        return newVisitCnt;
    }

    public void setNewVisitCnt(int newVisitCnt) {
        this.newVisitCnt = newVisitCnt;
    }

    public int getPageview_cnt() {
        return pageview_cnt;
    }

    public void setPageview_cnt(int pageview_cnt) {
        this.pageview_cnt = pageview_cnt;
    }

    public long getStayTime() {
        return stayTime;
    }

    public void setStayTime(long stayTime) {
        this.stayTime = stayTime;
    }

    public int getInboundPageCnt() {
        return inboundPageCnt;
    }

    public void setInboundPageCnt(int inboundPageCnt) {
        this.inboundPageCnt = inboundPageCnt;
    }

    public int getOutboundPageCnt() {
        return outboundPageCnt;
    }

    public void setOutboundPageCnt(int outboundPageCnt) {
        this.outboundPageCnt = outboundPageCnt;
    }

    public int getBounceCnt() {
        return bounceCnt;
    }

    public void setBounceCnt(int bounceCnt) {
        this.bounceCnt = bounceCnt;
    }

    public int getBasketClickCnt() {
        return basketClickCnt;
    }

    public void setBasketClickCnt(int basketClickCnt) {
        this.basketClickCnt = basketClickCnt;
    }

    public int getZzimClickCnt() {
        return zzimClickCnt;
    }

    public void setZzimClickCnt(int zzimClickCnt) {
        this.zzimClickCnt = zzimClickCnt;
    }

    // For aggregation
    public void addNewVisitCnt(int newVisitCnt) {
        this.newVisitCnt = this.newVisitCnt + newVisitCnt;
    }

    public void addPageview_cnt(int pageview_cnt) {
        this.pageview_cnt = this.pageview_cnt + pageview_cnt;
    }

    public void addStayTime(long stayTime) {
        this.stayTime = this.stayTime + stayTime;
    }

    public void addInboundPageCnt(int inboundPageCnt) {
        this.inboundPageCnt = this.inboundPageCnt + inboundPageCnt;
    }

    public void addOutboundPageCnt(int endPageCnt) {
        this.outboundPageCnt = this.outboundPageCnt + endPageCnt;
    }

    public void addBounceCnt(int bounceCnt) {
        this.bounceCnt = this.bounceCnt + bounceCnt;
    }

    public void addBasketClickCnt(int basketClickCnt) {
        this.basketClickCnt = this.basketClickCnt + basketClickCnt;
    }

    public void addZzimClickCnt(int zzimClickCnt) {
        this.zzimClickCnt = this.zzimClickCnt + zzimClickCnt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ContentVisit [ custId=" + custId);
        sb.append(", cookieId=" + cookieId);
        sb.append(", currPageUri=" + currPageUri);
        sb.append(", productCd=" + productCd);
        sb.append(", categoryCdL=" + categoryCdL);
        sb.append(", categoryCdM=" + categoryCdM);
        sb.append(", categoryCdS=" + categoryCdS);
        sb.append(", newVisitCnt=" + newVisitCnt);
        sb.append(", pageview_cnt=" + pageview_cnt);
        sb.append(", stayTime=" + stayTime);
        sb.append(", inboundPageCnt=" + inboundPageCnt);
        sb.append(", outboundPageCnt=" + outboundPageCnt);
        sb.append(", bounceCnt=" + bounceCnt);
        sb.append(", basketClickCnt=" + basketClickCnt);
        sb.append(", zzimClickCnt=" + zzimClickCnt);
        sb.append("]");
        return sb.toString();
    }
}