package com.obzen.spark.batch.inykang.parser;

import com.google.common.collect.Lists;
import com.obzen.spark.batch.inykang.model.*;

import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by inykang on 17. 4. 28.
 */
public class ProductOrderParser implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ProductOrderParser.class);
    private Pattern pattern = Pattern.compile(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))");

    // 기본로그(Base) //
    public LogBaseDetail mapLogBaseDetail(Row row) {
        return new LogBaseDetail(
                Objects.toString(row.getAs("event_timestamp"), "")
                , Objects.toString(row.getAs("session_id"), "")
                , Objects.toString(row.getAs("uuid"), "")
                , Objects.toString(row.getAs("cust_id"), "")
                , Objects.toString(row.getAs("cookie_id"), "")
                , Objects.toString(row.getAs("gender"), "")
                , Objects.toString(row.getAs("age"), "")
                , Objects.toString(row.getAs("area"), "")
                , Objects.toString(row.getAs("grade"), "")
                , Objects.toString(row.getAs("event_type_cd"), "")
                , Objects.toString(row.getAs("click_cd"), "")
                , Objects.toString(row.getAs("device_type"), "")
                , Objects.toString(row.getAs("app_web_type"), "")
                , Objects.toString(row.getAs("browser_type"), "")
                , Objects.toString(row.getAs("ip_addr"), "")
                , Objects.toString(row.getAs("inbound_chnl_src"), "")
                , Objects.toString(row.getAs("inbound_chnl_medium"), "")
                , Objects.toString(row.getAs("inbound_chnl_campaign"), "")
                , Objects.toString(row.getAs("inbound_chnl_keyword"), "")
                , Objects.toString(row.getAs("referrer_uri"), "")
                , Objects.toString(row.getAs("curr_page_uri"), "")
                , Objects.toString(row.getAs("pv_type"), "")
                , Objects.toString(row.getAs("searchword"), "")
                , Objects.toString(row.getAs("search_result_num"), "")
                , Objects.toString(row.getAs("product_cd"), "")
                , Objects.toString(row.getAs("product_catg_l"), "")
                , Objects.toString(row.getAs("product_catg_m"), "")
                , Objects.toString(row.getAs("product_catg_s"), "")
                , Objects.toString(row.getAs("product_brand_cd"), "")
                , Objects.toString(row.getAs("cart_id"), "")
                , Objects.toString(row.getAs("order_id"), "")
        );
    }
    // 기본로그(Base) //

    // 주문로그(Order) //
    public OrgOrderLog mapOrgOrderLog(Row row) {
        return new OrgOrderLog(
                Objects.toString(row.getAs("event_timestamp"), "")
                , Objects.toString(row.getAs("session_id"), "")
                , Objects.toString(row.getAs("order_id"), "")
                , Objects.toString(row.getAs("cart_id"), "")
                , row.getAs("product_seq")
                , Objects.toString(row.getAs("product_cd"), "")
                , row.getAs("product_qty")
                , row.getAs("product_unit_price")
        );
    }
    // 주문로그(Order) //

    // 상품정보 //
    public ProductInfo mapProductInfo(String line) {
        String[] data = pattern.split(line);
        return new ProductInfo(
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]
        );
    }
    // 상품정보 //

    // 고객별상품별주문집계(CustProductOrder) //
    public Iterable<CustProductOrder> mapCustProductOrder(
            String date
            , Tuple2<Iterable<OrgOrderLog>, Iterable<LogBaseDetail>> tuple
            , Map<String, ProductInfo> productMap
    ) {
        List<OrgOrderLog> orderList = Lists.newArrayList(tuple._1());
        List<LogBaseDetail> baseList = Lists.newArrayList(tuple._2());

        String custId = searchCustId(baseList);
        String cookie = searchCookie(baseList);

        Map<String, CustProductOrder> productOrderMap = new HashMap<>();
        for (OrgOrderLog order : orderList) {
            String productCd = order.getProductCd();
            CustProductOrder productOrder = productOrderMap.get(productCd);
            if (productOrder == null) {
                // new product order object with the product cd
                ProductInfo info = productMap.get(productCd);
                productOrder = createCustProductOrder(date, custId, cookie, order, info);
                productOrderMap.put(productCd, productOrder);

            } else {
                // add order count, amount into present one
                productOrder.addOrder_cnt(1);
                productOrder.addAmount(
                        order.getProductQty() * order.getProductUnitPrice()
                );
            }
        }

        return productOrderMap.values();
    }

    private CustProductOrder createCustProductOrder(String date, String custId
            , String cookie, OrgOrderLog order, ProductInfo info) {
        return new CustProductOrder(
                date
                , custId
                , cookie
                , order.getProductCd()
                , (info==null)? "" : info.getProductNm()
                , (info==null)? "" : info.getCategoryCdL()
                , (info==null)? "" : info.getCategoryCdM()
                , (info==null)? "" : info.getCategoryCdS()
                , ""
                , (info==null)? "" : info.getCategoryNmL()
                , (info==null)? "" : info.getCategoryNmM()
                , (info==null)? "" : info.getCategoryNmS()
                , ""
                , 1
                , order.getProductUnitPrice() * order.getProductQty()
        );
    }
    // 고객별상품별주문집계(CustProductOrder) //

    // 상품별주문요약(ProductOrderSummary) //
    public ProductOrderSummary mapProductOrderSummary(CustProductOrder productOrder) {
        return new ProductOrderSummary(
                productOrder.getBase_dt()
                , productOrder.getProduct_cd()
                , productOrder.getProduct_nm()
                , productOrder.getProduct_catg_l()
                , productOrder.getProduct_catg_m()
                , productOrder.getProduct_catg_s()
                , productOrder.getProduct_brand_cd()
                , productOrder.getProduct_catg_l_nm()
                , productOrder.getProduct_catg_m_nm()
                , productOrder.getProduct_catg_s_nm()
                , productOrder.getProduct_brand_nm()
                , productOrder.getOrder_cnt()
                , productOrder.getAmount()
        );
    }
    // 상품별주문요약(ProductOrderSummary) //

    // 컨텐츠별주문요약(ContentOrderSummary) //
    public ContentOrderSummary mapContentOrderSummary(String date
            , Map<String, ProductInfo> productMap, Tuple2<String, Integer> tuple) {
        String productCd = tuple._1();
        ProductInfo info = productMap.get(productCd);

        return new ContentOrderSummary (
                date
                , productCd
                , (info==null)? "" : info.getProductNm()
                , (info==null)? "" : info.getCategoryCdL()
                , (info==null)? "" : info.getCategoryCdM()
                , (info==null)? "" : info.getCategoryCdS()
                , (info==null)? "" : info.getCategoryNmL()
                , (info==null)? "" : info.getCategoryNmM()
                , (info==null)? "" : info.getCategoryNmS()
                , tuple._2()
        );
    }

    private String searchCustId(List<LogBaseDetail> baseDetails) {
        String custId = "";
        for (LogBaseDetail base : baseDetails) {
            if (!isEmpty(base.getCustId())) {
                custId = base.getCustId();
                break;
            }
        }
        return custId;
    }

    private String searchCookie(List<LogBaseDetail> baseDetails) {
        String cookie = "";
        for (LogBaseDetail base : baseDetails) {
            if (!isEmpty(base.getCookieId())) {
                cookie = base.getCookieId();
                break;
            }
        }
        return cookie;
    }

    private boolean isEmpty(String value) {
        return (value == null || value.trim().equals(""));
    }

    private String nullToEmpty(String value) {
        return (value == null || value.equalsIgnoreCase("null"))
                ? "" : value.trim();
    }
}