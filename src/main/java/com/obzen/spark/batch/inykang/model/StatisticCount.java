package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class StatisticCount implements Serializable {
    private int visit = 0;
    private int visitor = 0;
    private int customer = 0;
    private int newVisitor = 0;
    private int pageView = 0;
    private int page = 0;
    private long stayTime = 0L;
    private int bounce = 0;
    private int order = 0;
    private long amount = 0L;

    public int getVisit() {
        return visit;
    }

    public void setVisit(int visit) {
        this.visit = visit;
    }

    public void addVisit(int visit) {
        this.visit = this.visit + visit;
    }

    public int getVisitor() {
        return visitor;
    }

    public void addVisitor(int visitor) {
        this.visitor = this.visitor + visitor;
    }

    public int getCustomer() {
        return customer;
    }

    public void setCustomer(int customer) {
        this.customer = customer;
    }

    public void addCustomer(int customer) {
        this.customer = this.customer + customer;
    }

    public int getNewVisitor() {
        return newVisitor;
    }

    public void addNewVisitor(int newVisitor) {
        this.newVisitor = this.newVisitor + newVisitor;
    }

    public int getPageView() {
        return pageView;
    }

    public void addPageView(int pageView) {
        this.pageView = this.pageView + pageView;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public void addPage(int page) {
        this.page = this.page + page;
    }

    public long getStayTime() {
        return stayTime;
    }

    public void addStayTime(long stayTime) {
        this.stayTime = this.stayTime + stayTime;
    }

    public int getBounce() {
        return bounce;
    }

    public void addBounce(int bounce) {
        this.bounce = this.bounce + bounce;
    }

    public int getOrder() {
        return order;
    }

    public void addOrder(int order) {
        this.order = this.order + order;
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
        sb.append("StatisticCount [ visit=" + visit);
        sb.append(", visitor=" + visitor);
        sb.append(", customer=" + customer);
        sb.append(", newVisitor=" + newVisitor);
        sb.append(", pageView=" + pageView);
        sb.append(", page=" + page);
        sb.append(", stayTime=" + stayTime);
        sb.append(", bounce=" + bounce);
        sb.append(", order=" + order);
        sb.append(", amount=" + amount);
        return sb.toString();
    }
}
