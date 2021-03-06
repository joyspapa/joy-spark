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

package com.obzen.spark.batch.inykang.model;

import java.io.Serializable;

/**
 * OrgOrderLog definition.
 * <p>
 * Code generated by Apache Ignite Schema Import utility: 01/20/2017.
 */
public class OrgOrderLog implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Value for eventTimestamp.
     */
    private String eventTimestamp;

    /**
     * Value for sessionId.
     */
    private String sessionId;

    /**
     * Value for orderId.
     */
    private String orderId;

    /**
     * Value for cartId.
     */
    private String cartId;

    /**
     * Value for productSeq.
     */
    private int productSeq;

    /**
     * Value for productCd.
     */
    private String productCd;

    /**
     * Value for productQty.
     */
    private int productQty;

    /**
     * Value for productUnitPrice.
     */
    private int productUnitPrice;

    /**
     * Empty constructor.
     */
    public OrgOrderLog() {
        // No-op.
    }

    /**
     * Full constructor.
     */
    public OrgOrderLog(
            String eventTimestamp,
            String sessionId,
            String orderId,
            String cartId,
            int productSeq,
            String productCd,
            int productQty,
            int productUnitPrice
    ) {
        this.eventTimestamp = eventTimestamp;
        this.sessionId = sessionId;
        this.orderId = orderId;
        this.cartId = cartId;
        this.productSeq = productSeq;
        this.productCd = productCd;
        this.productQty = productQty;
        this.productUnitPrice = productUnitPrice;
    }

    /**
     * Gets eventTimestamp.
     *
     * @return Value for eventTimestamp.
     */
    public String getEventTimestamp() {
        return eventTimestamp;
    }

    /**
     * Sets eventTimestamp.
     *
     * @param eventTimestamp New value for eventTimestamp.
     */
    public void setEventTimestamp(String eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    /**
     * Gets sessionId.
     *
     * @return Value for sessionId.
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Sets sessionId.
     *
     * @param sessionId New value for sessionId.
     */
    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    /**
     * Gets orderId.
     *
     * @return Value for orderId.
     */
    public String getOrderId() {
        return orderId;
    }

    /**
     * Sets orderId.
     *
     * @param orderId New value for orderId.
     */
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    /**
     * Gets cartId.
     *
     * @return Value for cartId.
     */
    public String getCartId() {
        return cartId;
    }

    /**
     * Sets cartId.
     *
     * @param cartId New value for cartId.
     */
    public void setCartId(String cartId) {
        this.cartId = cartId;
    }

    /**
     * Gets productSeq.
     *
     * @return Value for productSeq.
     */
    public int getProductSeq() {
        return productSeq;
    }

    /**
     * Sets productSeq.
     *
     * @param productSeq New value for productSeq.
     */
    public void setProductSeq(int productSeq) {
        this.productSeq = productSeq;
    }



    /**
     * Gets productCd.
     *
     * @return Value for productCd.
     */
    public String getProductCd() {
        return productCd;
    }

    /**
     * Sets productCd.
     *
     * @param productCd New value for productCd.
     */
    public void setProductCd(String productCd) {
        this.productCd = productCd;
    }

    /**
     * Gets productQty.
     *
     * @return Value for productQty.
     */
    public int getProductQty() {
        return productQty;
    }

    /**
     * Sets productQty.
     *
     * @param productQty New value for productQty.
     */
    public void setProductQty(int productQty) {
        this.productQty = productQty;
    }

    /**
     * Gets productUnitPrice.
     *
     * @return Value for productUnitPrice.
     */
    public int getProductUnitPrice() {
        return productUnitPrice;
    }

    /**
     * Sets productUnitPrice.
     *
     * @param productUnitPrice New value for productUnitPrice.
     */
    public void setProductUnitPrice(int productUnitPrice) {
        this.productUnitPrice = productUnitPrice;
    }

    public long getAmount() {
        return (long) (this.getProductQty() * this.productUnitPrice);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("OrgOrderLog [eventTimestamp=" + eventTimestamp);
        sb.append(", sessionId=" + sessionId);
        sb.append(", orderId=" + orderId);
        sb.append(", cartId=" + cartId);
        sb.append(", productSeq=" + productSeq);
        sb.append(", amount=" + this.getAmount());
        sb.append("]");
        return sb.toString();
    }
}