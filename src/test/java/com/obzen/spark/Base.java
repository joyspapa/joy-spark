package com.obzen.spark;

import java.io.Serializable;

public class Base implements Serializable {
    private String id;
    private int value;
    private int count;
    private int sum;
    private double avg;

    public Base() {
    }

    ;

    public Base(String id, int value, int count, int sum, int avg) {
        this.id = id;
        this.value = value;
        this.count = count;
        this.sum = value;
        this.avg = avg;
    }

    public String id() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int value() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int count() {
        return count;
    }

    public void addCount(int count) {
        this.count = this.count + count;
    }

    public int sum() {
        return sum;
    }

    public void addSum(int sum) {
        this.sum = this.sum + sum;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    @Override
    public String toString() {
        return String.format("[id:%s, value:%d, count:%d, sum:%d, avg:%.2f]"
                , id, value, count, sum, avg);
    }
}
