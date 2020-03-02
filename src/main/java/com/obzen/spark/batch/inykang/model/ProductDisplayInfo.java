package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

public class ProductDisplayInfo implements Serializable {
    private String prodCatgCd;
    private String prodCatgType;
    private String prodCatgNm;

    public ProductDisplayInfo(
            String prodCatgCd,
            String prodCatgType,
            String prodCatgNm
    ) {
        this.prodCatgCd = prodCatgCd;
        this.prodCatgType = prodCatgType;
        this.prodCatgNm = prodCatgNm;
    }

    public String getProdCatgCd() {
        return prodCatgCd;
    }

    public void setProdCatgCd(String prodCatgCd) {
        this.prodCatgCd = prodCatgCd;
    }

    public String getProdCatgType() {
        return prodCatgType;
    }

    public void setProdCatgType(String prodCatgType) {
        this.prodCatgType = prodCatgType;
    }

    public String getProdCatgNm() {
        return prodCatgNm;
    }

    public void setProdCatgNm(String prodCatgNm) {
        this.prodCatgNm = prodCatgNm;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ProductDisplayInfo [ prodCatgCd=" + prodCatgCd);
        sb.append(", prodCatgType=" + prodCatgType);
        sb.append(", prodCatgNm=" + prodCatgNm);
        sb.append("]");
        return sb.toString();
    }
}
