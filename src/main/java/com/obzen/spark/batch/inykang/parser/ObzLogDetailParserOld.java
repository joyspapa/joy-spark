package com.obzen.spark.batch.inykang.parser;

import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obzen.spark.batch.inykang.model.*;

import scala.Tuple2;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by inykang on 17. 4. 28.
 */
public class ObzLogDetailParserOld implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ObzLogDetailParserOld.class);
    private SimpleDateFormat format = new SimpleDateFormat("HHmmssSSS");
    private Pattern pattern = Pattern.compile(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))");

    public Iterable<LogBaseDetail> sortAndCalcStayingTime(Tuple2<Iterable<LogBaseDetail>
            , Optional<Iterable<OrgOrderLog>>> tuple) {

        // When orders exist
        Map<String, Long> orderAmountMap = new HashMap<>();
        if (tuple._2().isPresent()) {
            // Sort orders
            List<OrgOrderLog> orderList = new ArrayList<>();
            tuple._2().get().forEach(orderList::add);
            orderList.sort(
                    Comparator.comparing((OrgOrderLog o) -> o.getOrderId())
                            .thenComparingInt((OrgOrderLog o) -> o.getProductSeq())
            );
            // Map orderID, amount
            String orderId = null;
            long amount = 0L;
            for (OrgOrderLog order : tuple._2().get()) {
                if (isEmpty(orderId)) orderId = order.getOrderId();
                else if (orderId.equals(order.getOrderId())) {
                    amount = amount + order.getAmount();
                } else {
                    orderAmountMap.put(orderId, amount);
                    orderId = order.getOrderId();
                    amount = order.getAmount();
                }
            }
            orderAmountMap.put(orderId, amount);
        }


        // Sort and set baseDetails
        List<LogBaseDetail> baseDetailList = new ArrayList<>();
        tuple._1().forEach(baseDetailList::add);

        // Sort by timestamp
        baseDetailList.sort(
                Comparator.comparing((LogBaseDetail o) -> o.getEventTimestamp())
        );

        int pageSeq = 1;
        long prevTimestamp = 0L;
        long currTimestamp;
        String custId = "", age = "", place = "", custGrade = "";

        int size = baseDetailList.size();
        for (int i = 0; i < size; i++) {
            LogBaseDetail baseDetail = baseDetailList.get(i);

            // Set page sequence
            baseDetail.setPageSeq(pageSeq);
            pageSeq++;

            // Calculate staying time
            if (prevTimestamp == 0L) {
                prevTimestamp = parseTimeMillis(baseDetail.getEventTimestamp());
                baseDetail.setStayTime(0L);

            } else {
                currTimestamp = parseTimeMillis(baseDetail.getEventTimestamp());
                baseDetail.setStayTime(currTimestamp - prevTimestamp);
                prevTimestamp = currTimestamp;
            }

            // If log has order, set order amount
            if (!isEmpty(baseDetail.getOrderId()))
                baseDetail.setAmount(orderAmountMap.get(baseDetail.getOrderId()));

            // Find customer info.
            if (isEmpty(custId) && !isEmpty(baseDetail.getCustId())) {
                custId = baseDetail.getCustId();
                age = baseDetail.getAge();
                place = baseDetail.getPlace();
                custGrade = baseDetail.getCustGrade();
            }
        }

        // Fill out customer info and session ID
        for (LogBaseDetail base : baseDetailList) {
            if (isEmpty(base.getCustId())) {
                base.setCustId(custId);
                base.setAge(age);
                base.setPlace(place);
                base.setCustGrade(custGrade);
            }
        }

        return baseDetailList;
    }

    public VisitDetail mapVisitDetail(String date, Iterable<LogBaseDetail> baseIter) {
        List<LogBaseDetail> baseList = new ArrayList<>();
        baseIter.forEach(baseList::add);

        // Sort by timestamp
        baseList.sort(
                Comparator.comparingInt((LogBaseDetail o) -> o.getPageSeq())
        );

        LogBaseDetail first = baseList.get(0);
        VisitDetail visit = createVisitDetail(date, first);
        String cookieId = first.getCookieId();

        int size = baseList.size();

        // Page view count
        visit.setPageview_cnt(size);

        HashSet<String> uniquePages = new HashSet<>();
        for (int i = 0; i < size; i++) {
            LogBaseDetail base = baseList.get(i);
            // First log
            if (i == 0) {
                visit.setStart_time(base.getEventTimestamp());
                visit.setStart_page_uri(base.getCurrPageUri());
            }
            // Last log
            if (i == size - 1) {
                visit.setEnd_time(base.getEventTimestamp());
                visit.setEnd_page_uri(base.getCurrPageUri());
            }

            // Sum staying time
            visit.addStay_time(base.getStayTime());

            // Count unique pages
            uniquePages.add(base.getCurrPageUri());

            // Register visit
            String pvType = base.getPvType();
            if (visit.getRegist_visit_yn().equals("N") && isRegisterPage(pvType))
                visit.setRegist_visit_yn("Y");

            // Order count
            if (!isEmpty(pvType) && isOrderPage(pvType))
                visit.addOrder_cnt(1);

            // Sum order amount
            visit.addAmount(base.getAmount());

            // Find cookie ID
            if (isEmpty(cookieId) && !isEmpty(base.getCookieId()))
                cookieId = base.getCookieId();
        }

        // Set new visit
        visit.setNew_visit_yn(
                isNewVisit(
                first.getCookieId(), first.getCustId(), visit.getRegist_visit_yn()
                ) ? "Y" : "N"
        );

        // Set unique page count
        visit.setPage_cnt(uniquePages.size());

        // Set cookie id
        visit.setCookie_id(cookieId);

        return visit;
    }

    public CustVisitDaily mapCustVisitDaily(String date, Iterable<LogBaseDetail> baseIter) {
        List<LogBaseDetail> baseList = new ArrayList<>();
        baseIter.forEach(baseList::add);

        CustVisitDaily custVisit = createCustVisitDaily(date, baseList.get(0));
        StatisticCount stat = giveStatistics(baseList);
        custVisit.setVisit_cnt(stat.getVisit());
        custVisit.setPageview_cnt(stat.getPageView());
        custVisit.setPage_cnt(stat.getPage());
        custVisit.setStay_time(stat.getStayTime());
        custVisit.setBounce_cnt(stat.getBounce());
        custVisit.setOrder_visit_cnt(stat.getOrder());
        custVisit.setAmount(stat.getAmount());

        return custVisit;
    }

    public CustVisitSummary mapCustVisitSummaryDaily(String date, Iterable<LogBaseDetail> baseIter) {
        List<LogBaseDetail> baseList = new ArrayList<>();
        baseIter.forEach(baseList::add);

        CustVisitSummary custVisit = createCustVisitSummaryDaily(date, baseList.get(0));
        StatisticCount stat = giveStatistics(baseList);
        custVisit.setVisit_cnt(stat.getVisit());
        custVisit.setVisitor_cnt(stat.getVisitor());
        custVisit.setCust_cnt(stat.getCustomer());
        custVisit.setNew_visitor_cnt(stat.getNewVisitor());
        custVisit.setPageview_cnt(stat.getPageView());
        custVisit.setPage_cnt(stat.getPage());
        custVisit.setStay_time(stat.getStayTime());
        custVisit.setBounce_cnt(stat.getBounce());
        custVisit.setOrder_visit_cnt(stat.getOrder());
        custVisit.setAmount(stat.getAmount());

        return custVisit;
    }

    public DeviceVisitSummary mapDeviceVisitSummaryDaily(String date, Iterable<LogBaseDetail> baseIter) {
        List<LogBaseDetail> baseList = new ArrayList<>();
        baseIter.forEach(baseList::add);

        DeviceVisitSummary deviceVisit = createDeviceVisitSummaryDaily(date, baseList.get(0));
        StatisticCount stat = giveStatistics(baseList);
        deviceVisit.setVisit_cnt(stat.getVisit());
        deviceVisit.setVisitor_cnt(stat.getVisitor());
        deviceVisit.setCust_cnt(stat.getCustomer());
        deviceVisit.setNew_visitor_cnt(stat.getNewVisitor());
        deviceVisit.setPageview_cnt(stat.getPageView());
        deviceVisit.setPage_cnt(stat.getPage());
        deviceVisit.setStay_time(stat.getStayTime());
        deviceVisit.setBounce_cnt(stat.getBounce());
        deviceVisit.setOrder_visit_cnt(stat.getOrder());
        deviceVisit.setAmount(stat.getAmount());

        return deviceVisit;
    }

    public InChnlVisitSummary mapInChnlVisitSummaryDaily(String date, Iterable<LogBaseDetail> baseIter) {
        List<LogBaseDetail> baseList = new ArrayList<>();
        baseIter.forEach(baseList::add);

        InChnlVisitSummary inChalVisit = createInChnlVisitSummaryDaily(date, baseList.get(0));
        StatisticCount stat = giveStatistics(baseList);
//        inChalVisit.setVisit_cnt(stat.getVisit());
//        inChalVisit.setVisitor_cnt(stat.getVisitor());
//        inChalVisit.setCust_cnt(stat.getCustomer());
//        inChalVisit.setNew_visitor_cnt(stat.getNewVisitor());
//        inChalVisit.setPageview_cnt(stat.getPageView());
//        inChalVisit.setPage_cnt(stat.getPage());
//        inChalVisit.setStayTime(stat.getStayTime());
//        inChalVisit.setBounce_cnt(stat.getBounce());
//        inChalVisit.setOrder_visit_cnt(stat.getOrder());
//        inChalVisit.setAmount(stat.getAmount());

        return inChalVisit;
    }

    public CustSearchword mapCustSearchwordDaily(String date, Iterable<LogBaseDetail> baseIter) {
        List<LogBaseDetail> baseList = new ArrayList<>();
        baseIter.forEach(baseList::add);

        CustSearchword custSearchword = createCustSearchwordDaily(date, baseList.get(0));
        custSearchword.setSearch_cnt(baseList.size());
        for (LogBaseDetail base : baseList) {
            String searchRst = base.getSearchResultType();
            if (isEmpty(searchRst) || searchRst.trim().equals("0"))
                custSearchword.addSearch_fail_cnt(1);
        }

        return custSearchword;
    }

    public SearchwordSummary mapSearchwordSummaryDaily(CustSearchword searchword) {
        return new SearchwordSummary(
                searchword.getBase_dt()
                , searchword.getSearchword()
                , searchword.getSearch_cnt()
                , searchword.getSearch_fail_cnt()
        );
    }


    public SearchwordSummary reduceSearchwordSummaryDaily(SearchwordSummary v1, SearchwordSummary v2) {
        SearchwordSummary v = new SearchwordSummary(v1.getBase_dt()
                , v1.getSearchword(), v1.getSearch_cnt(), v1.getSearch_fail_cnt());
        v.addSearch_cnt(v1.getSearch_cnt() + v2.getSearch_cnt());
        v.addSearch_fail_cnt(v1.getSearch_fail_cnt() + v2.getSearch_fail_cnt());
        return v;
    }

    public Boolean hasProductCode(Tuple2<String, Iterable<LogBaseDetail>> tuple) {
        boolean hasProductCode = false;
        for (LogBaseDetail base : tuple._2())
            if (!isEmpty(base.getProductCd())) {
                hasProductCode = true;
                break;
            }
        return hasProductCode;
    }

    public Iterable<ContentVisit> mapContentVisit(Iterable<LogBaseDetail> baseIter) {
        List<LogBaseDetail> baseList = new ArrayList<>();
        baseIter.forEach(baseList::add);

        // Sort by page sequence
        baseList.sort(Comparator.comparingInt((LogBaseDetail o) -> o.getPageSeq()));

        LogBaseDetail first = baseList.get(0);
        boolean isRegister = false;
        // Check register visit
        for (LogBaseDetail base : baseList) {
            if (isRegisterPage(base.getPvType())) {
                isRegister = true;
                break;
            }
        }
        // Check new visit
        //boolean isNewVisit = isNewVisit(first.getCookieId(), first.getCustId(),isRegister);

        Map<String, ContentVisit> contentMap = new HashMap<>();
        String custId = searchCustId(baseList);
        String cookie = searchCookie(baseList);
        for (int i = 0; i < baseList.size(); i++) {
            LogBaseDetail base = baseList.get(i);
            String productCd = base.getProductCd();
            if (isEmpty(productCd)) {
                ContentVisit content = contentMap.get(productCd);
                if ( content == null) {
//                     content = new ContentVisit(
//                            base.getProductCd()
//                            , custId, cookie
//                            , isNewVisit
//                    );
                    contentMap.put(productCd, content);
                } else {
                    content.addPageview_cnt(1);
                    content.addStayTime(base.getStayTime());
                }

                // Inbound page
//                if (i == 0)
//                    content.setInboundPageCnt(true);
//                // End page
//                if (i == (baseList.size() - 1))
//                    content.setOutboundPageCnt(true);
//                // Bounce
//                if (content.getInboundPageCnt() + content.getOutboundPageCnt())
//                    content.setBounceCnt(true);
                // Order visit
                //if (isOrderPage(base.getPvType()))
                //   content.setOrder(true);
            }
        }
        return contentMap.values();
    }

    public ProductInfo mapProductInfo(String line) {
        String[] data = pattern.split(line);
        return new ProductInfo(
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]
        );
    }

    private StatisticCount giveStatistics(List<LogBaseDetail> baseList) {
        // Sort by timestamp, session ID
        baseList.sort(
                Comparator.comparing((LogBaseDetail o) -> o.getSessionId())
                        .thenComparing((LogBaseDetail o) -> o.getEventTimestamp())
        );

        HashSet<String> customerSet = new HashSet<>();
        HashSet<String> uniquePageSet = new HashSet<>();
        HashSet<String> cookieSet = new HashSet<>();

        // Group by session ID
        Map<String, List<LogBaseDetail>> sessionMap = baseList.stream()
                .collect(Collectors.groupingBy(LogBaseDetail::getSessionId));

        StatisticCount stat = new StatisticCount();

        // Visit count
        stat.setVisit(sessionMap.size());

        for (List<LogBaseDetail> baseDetails : sessionMap.values()) {
            // Visitor count
            String cust = searchCustId(baseDetails);
            String cookie = searchCookie(baseDetails);
            if (isEmpty(cust)) {
                if (isEmpty(cookie))
                    stat.addVisitor(1);
                else {
                    cookieSet.add(cookie);
                }
            } else {
                if (!cookieSet.contains(cookie))
                    customerSet.add(cust);
            }
            // Page view count
            stat.addPageView(baseDetails.size());

            // Count bounce
            if (baseDetails.size() == 1)
                stat.addBounce(1);

            boolean isNewVisit = false;
            boolean isOrderVisit = false;
            for (LogBaseDetail baseDetail : baseDetails) {
                // Count page
                uniquePageSet.add(baseDetail.getCurrPageUri());
                // Sum staying time
                stat.addStayTime(baseDetail.getStayTime());

                if (!isNewVisit && isNewVisit(baseDetail.getCookieId(), baseDetail.getCustId()
                        , isRegisterPage(baseDetail.getPvType())? "Y" : "N"))
                    isNewVisit = true;
                if (!isOrderVisit && isOrderPage(baseDetail.getPvType()))
                    isOrderVisit = true;

            }
            // New visit
            if (isNewVisit) stat.addNewVisitor(1);
            // Order visit
            if (isOrderVisit) stat.addOrder(1);
        }
        // Visitor count
        stat.addVisitor(customerSet.size() + cookieSet.size());
        // Customer visit
        stat.setCustomer(customerSet.size());
        // Page count
        stat.setPage(uniquePageSet.size());

        return stat;
    }

    private boolean isNewVisit(String cookieId, String custId, String isRegistVisit) {
        boolean isNewVisit = false;
        if (isEmpty(cookieId)) {
            if (isEmpty(custId))
                isNewVisit = true;
            else if (isRegistVisit.equals("Y"))
                isNewVisit = true;
        }
        return isNewVisit;
    }

    private VisitDetail createVisitDetail(String date, LogBaseDetail base) {
        return new VisitDetail(
                date
                , base.getSessionId()
                , base.getUuid()
                , base.getCustId()
                , base.getCookieId()
                , base.getSex()
                , base.getAge()
                , base.getPlace()
                , base.getCustGrade()
                , base.getDeviceType()
                , base.getAppWebType()
                , base.getIpAddr()
                , base.getInboundChnlSrc()
                , base.getInboundChnlMedium()
                , base.getInboundChnlCampaign()
                , base.getInboundChnlKeyword()
        );
    }

    private CustVisitDaily createCustVisitDaily(String date, LogBaseDetail base) {
        return new CustVisitDaily(
                date
                , base.getCustId()
                , base.getCookieId()
                , base.getSex()
                , base.getAge()
                , base.getPlace()
                , base.getCustGrade()
        );
    }

    private CustVisitSummary createCustVisitSummaryDaily(String date, LogBaseDetail base) {
        return new CustVisitSummary(
                date
                , base.getSex()
                , base.getAge()
                , base.getPlace()
                , base.getCustGrade()
        );
    }

    private DeviceVisitSummary createDeviceVisitSummaryDaily(String date, LogBaseDetail base) {
        return new DeviceVisitSummary(
                date
                , base.getDeviceType()
                , base.getAppWebType()
        );
    }

    private InChnlVisitSummary createInChnlVisitSummaryDaily(String date, LogBaseDetail base) {
        return null;
//        return new InChnlVisitSummary(
//                date
//                , base.getInboundChnlSrc()
//                , base.getInboundChnlMedium()
//                , base.getInboundChnlCampaign()
//                , base.getInboundChnlKeyword()
//        );
    }

    private CustSearchword createCustSearchwordDaily(String date, LogBaseDetail base) {
        return new CustSearchword(
                date
                , base.getCustId()
                , base.getCookieId()
                , base.getSearchWord()
        );
    }

    private long parseTimeMillis(String eventTimestamp) {
        long millis = 0L;
        try {
            millis = format.parse(eventTimestamp).getTime();
        } catch (ParseException e) {
            logger.error(String.format("Wrong date format! input:%s", eventTimestamp), e);
        }
        return millis;
    }

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

    private boolean isRegisterPage(String pvType) {
        return (!isEmpty(pvType) && pvType.equals("52"));
    }

    private boolean isOrderPage(String pvType) {
        return (!isEmpty(pvType) && pvType.equals("14"));
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