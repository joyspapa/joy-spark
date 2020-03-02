package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class ProductInfo implements Serializable{
    private String productCd;
    private String productNm;
    private String categoryCdL;
    private String categoryNmL;
    private String categoryCdM;
    private String categoryNmM;
    private String categoryCdS;
    private String categoryNmS;

    public ProductInfo (
            String productCd,
            String productNm,
            String categoryCdL,
            String categoryNmL,
            String categoryCdM,
            String categoryNmM,
            String categoryCdS,
            String categoryNmS
    ) {
        this.productCd = productCd;
        this.productNm = productNm;
        this.categoryCdL = categoryCdL;
        this.categoryNmL = categoryNmL;
        this.categoryCdM = categoryCdM;
        this.categoryNmM = categoryNmM;
        this.categoryCdS = categoryCdS;
        this.categoryNmS = categoryNmS;
    }

    public String getProductCd() {
        return productCd;
    }

    public void setProductCd(String productCd) {
        this.productCd = productCd;
    }

    public String getProductNm() {
        return productNm;
    }

    public void setProductNm(String productNm) {
        this.productNm = productNm;
    }

    public String getCategoryCdL() {
        return categoryCdL;
    }

    public void setCategoryCdL(String categoryCdL) {
        this.categoryCdL = categoryCdL;
    }

    public String getCategoryNmL() {
        return categoryNmL;
    }

    public void setCategoryNmL(String categoryNmL) {
        this.categoryNmL = categoryNmL;
    }

    public String getCategoryCdM() {
        return categoryCdM;
    }

    public void setCategoryCdM(String categoryCdM) {
        this.categoryCdM = categoryCdM;
    }

    public String getCategoryNmM() {
        return categoryNmM;
    }

    public void setCategoryNmM(String categoryNmM) {
        this.categoryNmM = categoryNmM;
    }

    public String getCategoryCdS() {
        return categoryCdS;
    }

    public void setCategoryCdS(String categoryCdS) {
        this.categoryCdS = categoryCdS;
    }

    public String getCategoryNmS() {
        return categoryNmS;
    }

    public void setCategoryNmS(String categoryNmS) {
        this.categoryNmS = categoryNmS;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ProductInfo [ productCd=" + productCd);
        sb.append(", productNm=" + productNm);
        sb.append(", categoryCdL=" + categoryCdL);
        sb.append(", categoryNmL=" + categoryNmL);
        sb.append(", categoryCdM=" + categoryCdM);
        sb.append(", categoryNmM=" + categoryNmM);
        sb.append(", categoryCdS=" + categoryCdS);
        sb.append(", categoryNmS=" + categoryNmS);
        sb.append("]");
        return sb.toString();
    }


}
