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

/**
 * Created by inykang on 17. 4. 28.
 */
public class LogDetailParser implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(LogDetailParser.class);
    private SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    private Pattern pattern = Pattern.compile(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*(?![^\\\"]*\\\"))");
    private Map<String, String> scenarioSeqMap = new HashMap<>();

    private SimpleDateFormat hourF = new SimpleDateFormat("HH");
    private SimpleDateFormat weekF = new SimpleDateFormat("EEE");

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
                        first.getCookieId(), first.getCustId(), visit.getRegist_visit_yn().equals("Y")
                ) ? "Y" : "N"
        );

        // Set unique page count
        visit.setPage_cnt(uniquePages.size());

        // Set cookie id
        visit.setCookie_id(cookieId);

        return visit;
    }

    // 고객병방문집계(CustVisitDaily) //
    public Tuple2<String, CustVisitDaily> groupingCustVisit(VisitDetail visit) {
        CustVisitDaily visitDaily = createCustVisitDaily(visit);
        // Bounce count
        if (visit.getPageview_cnt() == 1)
            visitDaily.setBounce_cnt(1);

        // Create group key
        String key = "";
        if (isEmpty(visit.getCust_id())) {
            if (!isEmpty(visit.getCookie_id()))
                key = visit.getCookie_id();
        } else {
            key = visit.getCust_id();
        }

        return new Tuple2<>(key, visitDaily);
    }

    private CustVisitDaily createCustVisitDaily(VisitDetail visit) {
        return new CustVisitDaily(
                visit.getBase_dt()
                , visit.getCust_id()
                , visit.getCookie_id()
                , visit.getSex()
                , visit.getAge()
                , visit.getPlace()
                , visit.getCust_grade()
                , 1  // initial visit count
                , visit.getPageview_cnt()
                , visit.getPage_cnt()
                , visit.getStay_time()
                , 0  // initial bounce
                , visit.getOrder_cnt()
                , visit.getAmount()
        );
    }

    public CustVisitDaily sumCustVisitDaily(CustVisitDaily accum, CustVisitDaily curr) {
        accum.addVisit_cnt(curr.getVisit_cnt());
        accum.addPageview_cnt(curr.getPageview_cnt());
        accum.addPage_cnt(curr.getPage_cnt());
        accum.addStay_time(curr.getStay_time());
        accum.addBounce_cnt(curr.getBounce_cnt());
        accum.addOrder_visit_cnt(curr.getOrder_visit_cnt());
        return accum;
    }
    // 고객병방문집계(CustVisitDaily) //

    // VisitSummary //
    public VisitSummary mapVisitSummary(VisitDetail visit) {
        VisitSummary visitSumm = new VisitSummary(
                visit.getBase_dt()
                , visit.getSex()
                , visit.getAge()
                , visit.getPlace()
                , visit.getCust_grade()
                , visit.getDevice_type()
                , visit.getApp_web_type()
                , visit.getInbound_chnl_src()
                , visit.getInbound_chnl_medium()
                , visit.getInbound_chnl_campaign()
                , visit.getInbound_chnl_keyword()
                , 1
                , (visit.getNew_visit_yn().equalsIgnoreCase("Y")) ? 1 : 0
                , visit.getPageview_cnt()
                , visit.getPage_cnt()
                , visit.getStay_time()
                , (visit.getPageview_cnt() == 1) ? 1 : 0
                , (visit.getOrder_cnt() > 0) ? 1 : 0
                , visit.getAmount()
        );

        String custId = visit.getCust_id();
        if (!isEmpty(custId)) visitSumm.getCustIdSet().add(custId);
        String customerKey = extractCustomerKey(custId, visit.getCookie_id());
        if (!isEmpty(customerKey)) visitSumm.getCustomerKeySet().add(customerKey);

        return visitSumm;
    }

    public VisitSummary seqSumVisitSummary(VisitSummary acc, VisitSummary curr) {
        if (isEmpty(acc.getBase_dt())) {
            acc.setBase_dt(curr.getBase_dt());
            acc.setSex(curr.getSex());
            acc.setAge(curr.getAge());
            acc.setPlace(curr.getPlace());
            acc.setCust_grade(curr.getCust_grade());
            acc.setDevice_type(curr.getDevice_type());
            acc.setApp_web_type(curr.getApp_web_type());
            acc.setInbound_chnl_src(curr.getInbound_chnl_src());
            acc.setInbound_chnl_medium(curr.getInbound_chnl_medium());
            acc.setInbound_chnl_campaign(curr.getInbound_chnl_campaign());
            acc.setInbound_chnl_keyword(curr.getInbound_chnl_keyword());
        }

        // Sum sequential visit summary
        acc.addVisit_cnt(1);
        acc.addNew_visitor_cnt((curr.getNew_visitor_cnt()));
        acc.addPageview_cnt(curr.getPageview_cnt());
        acc.addPage_cnt(curr.getPage_cnt());
        acc.addStay_time(curr.getStay_time());
        acc.addBounce_cnt(curr.getBounce_cnt());
        acc.addOrder_visit_cnt(curr.getOrder_visit_cnt());
        acc.addAmount(curr.getAmount());

        // Unique customer key and cookie
        if (!curr.getCustIdSet().isEmpty())
            acc.getCustIdSet().addAll(curr.getCustIdSet());
        if (!curr.getCustomerKeySet().isEmpty())
            acc.getCustomerKeySet().addAll(curr.getCustomerKeySet());

        return acc;
    }

    public VisitSummary combineSumVisitSummary(VisitSummary acc1, VisitSummary acc2) {
        acc1.addVisit_cnt(acc2.getVisit_cnt());
        acc1.addNew_visitor_cnt((acc2.getNew_visitor_cnt()));
        acc1.addPageview_cnt(acc2.getPageview_cnt());
        acc1.addPage_cnt(acc2.getPage_cnt());
        acc1.addStay_time(acc2.getStay_time());
        acc1.addBounce_cnt(acc2.getBounce_cnt());
        acc1.addOrder_visit_cnt(acc2.getOrder_visit_cnt());
        acc1.addAmount(acc2.getAmount());

        // Unique customer key and cookie
        if (!acc2.getCustIdSet().isEmpty())
            acc1.getCustIdSet().addAll(acc2.getCustIdSet());
        if (!acc2.getCustomerKeySet().isEmpty())
            acc1.getCustomerKeySet().addAll(acc2.getCustomerKeySet());

        return acc1;
    }
    // VisitSummary //

    // 고객속성별방문요약(CustVisitSummary) //
    public CustVisitSummary mapCustVisitSummary(VisitSummary visit) {
        return new CustVisitSummary(
                visit.getBase_dt()
                , visit.getSex()
                , visit.getAge()
                , visit.getPlace()
                , visit.getCust_grade()
                , visit.getVisit_cnt()
                , visit.getCustomerKeySet().size()
                , visit.getCustIdSet().size()
                , visit.getNew_visitor_cnt()
                , visit.getPageview_cnt()
                , visit.getPage_cnt()
                , visit.getStay_time()
                , visit.getBounce_cnt()
                , visit.getOrder_visit_cnt()
                , visit.getAmount()
        );
    }

    public Tuple2<CustVisitSummary, Tuple2<String, String>> mapCustVisitSummary(VisitDetail visit) {
        Tuple2<String, String> customerKey = new Tuple2<>(visit.getCust_id(), visit.getCookie_id());
        return new Tuple2<>(new CustVisitSummary(
                visit.getBase_dt()
                , visit.getSex()
                , visit.getAge()
                , visit.getPlace()
                , visit.getCust_grade()
                , 1
                , 0
                , 0
                , (visit.getNew_visit_yn().equalsIgnoreCase("Y")) ? 1 : 0
                , visit.getPageview_cnt()
                , visit.getPage_cnt()
                , visit.getStay_time()
                , (visit.getPageview_cnt() == 1) ? 1 : 0
                , (visit.getOrder_cnt() > 0) ? 1 : 0
                , visit.getAmount()
        ), customerKey);
    }

    public Tuple2<CustVisitSummary, HashSet<String>> seqSumVisitSummary(
            Tuple2<CustVisitSummary, HashSet<String>> accT
            , Tuple2<CustVisitSummary, Tuple2<String, String>> currT) {

        String customerKey = extractCustomerKey(currT._2()._1(), currT._2()._1());
        accT._2().add(customerKey);

        if (isEmpty(accT._1().getBase_dt())) {
            accT._1().setBase_dt(currT._1().getBase_dt());
            accT._1().setSex(currT._1().getSex());
            accT._1().setAge(currT._1().getAge());
            accT._1().setPlace(currT._1().getPlace());
            accT._1().setCust_grade(currT._1().getCust_grade());
        }
        accT._1().addVisit_cnt(currT._1().getVisit_cnt());
        accT._1().addVisitor_cnt(currT._1().getVisitor_cnt());
        accT._1().addCust_cnt(currT._1().getCust_cnt());
        accT._1().addNew_visitor_cnt(currT._1().getNew_visitor_cnt());
        accT._1().addPageview_cnt(currT._1().getPageview_cnt());
        accT._1().addPage_cnt(currT._1().getPage_cnt());
        accT._1().addStay_time(currT._1().getStay_time());
        accT._1().addBounce_cnt(currT._1().getBounce_cnt());
        accT._1().addOrder_visit_cnt(currT._1().getOrder_visit_cnt());
        accT._1().addAmount(currT._1().getAmount());

        return accT;
    }


    public Tuple2<CustVisitSummary, HashSet<String>> combineSumVisitDetail(Tuple2<CustVisitSummary, HashSet<String>> t1
            , Tuple2<CustVisitSummary, HashSet<String>> t2) {
        // Unique customer key
        t1._2().addAll(t2._2());

        // Sum accumulations
        t1._1().addVisit_cnt(t2._1().getVisit_cnt());
        t1._1().addVisitor_cnt(t2._1().getVisitor_cnt());
        t1._1().addCust_cnt(t2._1().getCust_cnt());
        t1._1().addNew_visitor_cnt(t2._1().getNew_visitor_cnt());
        t1._1().addPageview_cnt(t2._1().getPageview_cnt());
        t1._1().addPage_cnt(t2._1().getPage_cnt());
        t1._1().addStay_time(t2._1().getStay_time());
        t1._1().addBounce_cnt(t2._1().getBounce_cnt());
        t1._1().addOrder_visit_cnt(t2._1().getOrder_visit_cnt());
        t1._1().addAmount(t2._1().getAmount());

        return t1;
    }

    private CustVisitSummary createCustVisitSummaryDaily(VisitDetail visit) {
        return new CustVisitSummary(
                visit.getBase_dt()
                , visit.getSex()
                , visit.getAge()
                , visit.getPlace()
                , visit.getCust_grade()
                , 1  // initial visit count
                , 0  // initial visitor
                , 0  // initial customer
                , 0 // initial new visitor
                , visit.getPageview_cnt()
                , visit.getPage_cnt()
                , visit.getStay_time()
                , 0  // initial bounce
                , 0 // initial order visit
                , visit.getAmount()
        );
    }

    public CustVisitSummary mapCustVisitSummaryDaily(Iterable<VisitDetail> visitIter) {
        List<VisitDetail> visitList = new ArrayList<>();
        visitIter.forEach(visitList::add);

        CustVisitSummary visitSumm = createCustVisitSummaryDaily(visitList.get(0));
        StatisticCount stat = giveStatistics(visitList);
        visitSumm.setVisit_cnt(stat.getVisit());
        visitSumm.setVisitor_cnt(stat.getVisitor());
        visitSumm.setCust_cnt(stat.getCustomer());
        visitSumm.setNew_visitor_cnt(stat.getNewVisitor());
        visitSumm.setPageview_cnt(stat.getPageView());
        visitSumm.setPage_cnt(stat.getPage());
        visitSumm.setStay_time(stat.getStayTime());
        visitSumm.setBounce_cnt(stat.getBounce());
        visitSumm.setOrder_visit_cnt(stat.getOrder());
        visitSumm.setAmount(stat.getAmount());

        return visitSumm;
    }

    private CustVisitSummary createCustVisitSummaryDaily(CustVisitDaily visit) {
        return new CustVisitSummary(
                visit.getBase_dt()
                , visit.getSex()
                , visit.getAge()
                , visit.getPlace()
                , visit.getCust_grade()
        );
    }
    // 고객속성별방문요약(CustVisitSummary) //


    // 디바이스별방문요약(DeviceVisitSummary) //
    public DeviceVisitSummary mapDeviceVisitSummary(VisitSummary visit) {
        return new DeviceVisitSummary(
                visit.getBase_dt()
                , visit.getDevice_type()
                , visit.getApp_web_type()
                , visit.getVisit_cnt()
                , visit.getCustomerKeySet().size()
                , visit.getCustIdSet().size()
                , visit.getNew_visitor_cnt()
                , visit.getPageview_cnt()
                , visit.getPage_cnt()
                , visit.getStay_time()
                , visit.getBounce_cnt()
                , visit.getOrder_visit_cnt()
                , visit.getAmount()
        );
    }

    public Tuple2<DeviceVisitSummary, Tuple2<String, String>> mapDeviceVisitSummary(VisitDetail visit) {
        Tuple2<String, String> customerKey = new Tuple2<>(visit.getCust_id(), visit.getCookie_id());

        return new Tuple2<>(new DeviceVisitSummary(
                visit.getBase_dt()
                , visit.getDevice_type()
                , visit.getApp_web_type()
                , 1
                , 0
                , 0
                , (visit.getNew_visit_yn().equalsIgnoreCase("Y")) ? 1 : 0
                , visit.getPageview_cnt()
                , visit.getPage_cnt()
                , visit.getStay_time()
                , (visit.getPageview_cnt() == 1) ? 1 : 0
                , (visit.getOrder_cnt() > 0) ? 1 : 0
                , visit.getAmount()
        ), customerKey);
    }

    public DeviceVisitSummary mapDeviceVisitSummaryDaily(Iterable<VisitDetail> visitIter) {
        List<VisitDetail> visitList = new ArrayList<>();
        visitIter.forEach(visitList::add);

        DeviceVisitSummary deviceVisit = createDeviceVisitSummaryDaily(visitList.get(0));
        StatisticCount stat = giveStatistics(visitList);
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

    private DeviceVisitSummary createDeviceVisitSummaryDaily(VisitDetail visit) {
        return new DeviceVisitSummary(
                visit.getBase_dt()
                , visit.getDevice_type()
                , visit.getApp_web_type()
        );
    }
    // 디바이스별방문요약(DeviceVisitSummary) //

    // 유입채널별방문요약(InChnlVisitSummary) //
    public InChnlVisitSummary mapInChnlVisitSummary(VisitSummary visit) {
        return new InChnlVisitSummary(
                visit.getBase_dt()
                , visit.getInbound_chnl_src()
                , visit.getInbound_chnl_medium()
                , visit.getInbound_chnl_campaign()
                , visit.getInbound_chnl_keyword()
                , visit.getVisit_cnt()
                , visit.getCustomerKeySet().size()
                , visit.getCustIdSet().size()
                , visit.getNew_visitor_cnt()
                , visit.getPageview_cnt()
                , visit.getPage_cnt()
                , visit.getStay_time()
                , visit.getBounce_cnt()
                , visit.getOrder_visit_cnt()
                , visit.getAmount()
        );
    }
    /*
    public InChnlVisitSummary mapInChnlVisitSummary(Iterable<VisitDetail> visitIter) {
        List<VisitDetail> baseList = new ArrayList<>();
        visitIter.forEach(baseList::add);

        InChnlVisitSummary inChalVisit = createInChnlVisitSummaryDaily(baseList.get(0));
        StatisticCount stat = giveStatistics(baseList);
        inChalVisit.setVisit_cnt(stat.getVisit());
        inChalVisit.setVisitor_cnt(stat.getVisitor());
        inChalVisit.setCust_cnt(stat.getCustomer());
        inChalVisit.setNew_visitor_cnt(stat.getNewVisitor());
        inChalVisit.setPageview_cnt(stat.getPageView());
        inChalVisit.setPage_cnt(stat.getPage());
        inChalVisit.setStayTime(stat.getStayTime());
        inChalVisit.setBounce_cnt(stat.getBounce());
        inChalVisit.setOrder_visit_cnt(stat.getOrder());
        inChalVisit.setAmount(stat.getAmount());

        return inChalVisit;
    }


    private InChnlVisitSummary createInChnlVisitSummaryDaily(VisitDetail visit) {
        return new InChnlVisitSummary(
                visit.getBase_dt()
                , visit.getInbound_chnl_src()
                , visit.getInbound_chnl_medium()
                , visit.getInbound_chnl_campaign()
                , visit.getInbound_chnl_keyword()
        );
    }
    */
    // 유입채널별방문요약(InChnlVisitSummary) //

    // 고객별검색어별집계(CustSearchword) //
    public CustSearchword mapCustSearchwordDaily(String date, LogBaseDetail base) {
        return new CustSearchword(
                date
                , base.getCustId()
                , base.getCookieId()
                , base.getSearchWord()
                , 1
                , (base.getSearchResultType().equals("0")) ? 1 : 0
        );
    }

    public CustSearchword sumCustSearchword(CustSearchword acc, CustSearchword curr) {
        acc.addSearch_cnt(curr.getSearch_cnt());
        acc.addSearch_fail_cnt(curr.getSearch_fail_cnt());
        return acc;
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

    private CustSearchword createCustSearchwordDaily(String date, LogBaseDetail base) {
        return new CustSearchword(
                date
                , base.getCustId()
                , base.getCookieId()
                , base.getSearchWord()
        );
    }

    // 고객별검색어별집계(CustSearchword) //

    // 고객별검색어별집계(SearchwordSummary) //
    public SearchwordSummary mapSearchwordSummaryDaily(CustSearchword searchword) {
        return new SearchwordSummary(
                searchword.getBase_dt()
                , searchword.getSearchword()
                , searchword.getSearch_cnt()
                , searchword.getSearch_fail_cnt()
        );
    }

    public SearchwordSummary reduceSearchwordSummaryDaily(SearchwordSummary acc, SearchwordSummary curr) {
        acc.addSearch_cnt(curr.getSearch_cnt());
        acc.addSearch_fail_cnt(curr.getSearch_fail_cnt());
        return acc;
    }
    // 고객별검색어별집계(SearchwordSummary) //

    // 시나리오경로별방문목록(ScenarioPathVisit) //
    public boolean isScenarioPage(String pvType) {
        if (scenarioSeqMap.isEmpty()) createScenarioMap();
        return scenarioSeqMap.keySet().contains(pvType);
    }

    public ScenarioPathVisit mapScenarioPathVisit(String date, LogBaseDetail base) {
        return new ScenarioPathVisit(
                date
                , "상품구매"
                , getScenarioSequence(base.getPvType())
                , base.getPvType()
                , base.getSessionId()
                , base.getCustId()
                , base.getInboundChnlSrc()
                , base.getInboundChnlMedium()
                , base.getInboundChnlCampaign()
                , base.getSex()
                , base.getAge()
                , base.getDeviceType()
        );
    }

    public ScenarioPathVisit mergeScenarioPathVisit(ScenarioPathVisit acc, ScenarioPathVisit curr) {
        if (isEmpty(acc.getCust_id())) {
            acc.setCust_id(curr.getCust_id());
            acc.setSex(curr.getSex());
            acc.setAge(curr.getAge());
        }
        return acc;
    }

    private String getScenarioSequence(String pvType) {
        if (scenarioSeqMap.isEmpty())
            createScenarioMap();

        String seq = scenarioSeqMap.get(pvType);
        // temporally
        // input a sequence when it has no value.
        if (isEmpty(seq))
            seq = pvType;
        return seq;
    }

    private void createScenarioMap() {
        scenarioSeqMap.put("03", "1");
        scenarioSeqMap.put("11", "2");
        scenarioSeqMap.put("12", "3");
        scenarioSeqMap.put("13", "4");
        scenarioSeqMap.put("14", "5");
    }
    // 시나리오경로별방문목록(ScenarioPathVisit) //

    // 유입채널별시나리오경로별방문요약(InChnlScenarioPathSummary) //
    public InChnlScenarioPathSummary mapInChnlScenarioPathSummary(ScenarioPathVisit path) {
        return new InChnlScenarioPathSummary(
                path.getBase_dt()
                , path.getScenario_nm()
                , path.getScenario_page_seq()
                , path.getPv_type()
                , path.getInbound_chnl_src()
                , path.getInbound_chnl_medium()
                , path.getInbound_chnl_campaign()
                , 1
        );
    }

    public InChnlScenarioPathSummary sumInChnlScenarioPathSummary(InChnlScenarioPathSummary acc
            , InChnlScenarioPathSummary curr) {
        acc.addVisit_cnt(curr.getVisit_cnt());
        return acc;
    }
    // 유입채널별시나리오경로별방문요약(InChnlScenarioPathSummary) //

    // 상품정보, 상품디스플레이정보 //
    public Boolean hasProductCode(Tuple2<String, Iterable<LogBaseDetail>> tuple) {
        boolean hasProductCode = false;
        for (LogBaseDetail base : tuple._2())
            if (!isEmpty(base.getProductCd())) {
                hasProductCode = true;
                break;
            }
        return hasProductCode;
    }

    public ProductInfo mapProductInfo(String line) {
        String[] data = pattern.split(line);
        return new ProductInfo(
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]
        );
    }

    public ProductDisplayInfo mapProductDisplayInfo(String line) {
        String[] data = pattern.split(line);
        return new ProductDisplayInfo(data[0], data[1], data[2]);
    }
    // 상품정보, 상품디스플레이정보 //

    // 고객 상품 별 임시(ContentVisit) //
    public Iterable<ContentVisit> mapContentVisit(Iterable<LogBaseDetail> baseIter) {
        List<LogBaseDetail> baseList = new ArrayList<>();
        baseIter.forEach(baseList::add);

        // Sort by page sequence
        baseList.sort(Comparator.comparingInt((LogBaseDetail o) -> o.getPageSeq()));

        boolean isRegister = false;
        // Check register visit
        for (LogBaseDetail base : baseList) {
            if (isRegisterPage(base.getPvType())) {
                isRegister = true;
                break;
            }
        }
        // Check new visit
        LogBaseDetail first = baseList.get(0);
        boolean isNewVisit = isNewVisit(first.getCookieId(), first.getCustId(), isRegister);

        List<ContentVisit> contentList = new ArrayList<>();
        for (int i = 0; i < baseList.size(); i++) {
            LogBaseDetail base = baseList.get(i);

            //if (!isEmpty(base.getProductCd())) {
            ContentVisit content = new ContentVisit(
                    base.getSessionId()
                    , base.getCustId()
                    , base.getCookieId()
                    , base.getCurrPageUri()
                    , base.getProductCd()
                    , base.getProductCatgL()
                    , base.getProductCatgM()
                    , base.getProductCatgS()
                    , 0
                    , 1  // Initial count
                    , base.getStayTime()
                    , 0
                    , 0
                    , 0
                    , 0
                    , 0
            );
            // New visit count
            if (isNewVisit)
                content.setNewVisitCnt(1);
            // Inbound page
            if (i == 0)
                content.setInboundPageCnt(1);
            // Outbound page
            if (i == (baseList.size() - 1))
                content.setOutboundPageCnt(1);
            // Bounce
            if ((content.getInboundPageCnt() == 1)
                    && (content.getOutboundPageCnt() == 1))
                content.setBounceCnt(1);
            // Bascket click count
            if (isBasketClick(base.getEventTypeCd()))
                content.setBasketClickCnt(1);
            // Zimm click count
            if (isZzimClick(base.getEventTypeCd()))
                content.setZzimClickCnt(1);

            contentList.add(content);
            //}
        }
        return contentList;
    }
    // 고객 상품 별 임시(ContentVisit) //

    // 컨텐츠별방문요약(ContentVisitSummary) //
    public Tuple2<ContentVisit, Tuple2<HashSet<String>, HashSet<String>>> seqSumCustVisit(
            Tuple2<ContentVisit, Tuple2<HashSet<String>, HashSet<String>>> tuple
            , ContentVisit content
    ) {
        // Add customer key for counting unique customer
        tuple._2()._1().add(content.getCustId() + content.getCookieId());

        ContentVisit acc = tuple._1();
        if (isEmpty(acc.getSessionId())) {
            acc.setProductCd(content.getProductCd());
            acc.setCategoryCdL(content.getCategoryCdL());
            acc.setCategoryCdM(content.getCategoryCdM());
            acc.setCategoryCdS(content.getCategoryCdS());
            acc.setCurrPageUri(content.getCurrPageUri());
        }

        // Add session id for counting visit
        if (tuple._2()._2().add(content.getSessionId()))
            // New visitor count
            acc.addNewVisitCnt(content.getNewVisitCnt());
        // Page view count
        acc.addPageview_cnt(content.getPageview_cnt());
        // Staying time
        acc.addStayTime(content.getStayTime());
        // Inbound page count
        acc.addInboundPageCnt(content.getInboundPageCnt());
        // Outbound page count
        acc.addOutboundPageCnt(content.getOutboundPageCnt());
        // Bounce count
        acc.addBounceCnt(content.getBounceCnt());

        return tuple;
    }

    public Tuple2<ContentVisit, Tuple2<HashSet<String>, HashSet<String>>> combineSumCustVisit(
            Tuple2<ContentVisit, Tuple2<HashSet<String>, HashSet<String>>> accTuple
            , Tuple2<ContentVisit, Tuple2<HashSet<String>, HashSet<String>>> currTuple
    ) {
        ContentVisit acc = accTuple._1();
        ContentVisit curr = currTuple._1();

        // Add customer key set
        accTuple._2()._1().addAll(currTuple._2()._1());

        // Add session id for counting visit
        for (String session : currTuple._2()._2())
            if (accTuple._2()._2().add(session))
                // New visitor count
                acc.addNewVisitCnt(curr.getNewVisitCnt());

        // Page view count
        acc.addPageview_cnt(curr.getPageview_cnt());
        // Staying time
        acc.addStayTime(curr.getStayTime());
        // Inbound page count
        acc.addInboundPageCnt(curr.getInboundPageCnt());
        // Outbound page count
        acc.addOutboundPageCnt(curr.getOutboundPageCnt());
        // Bounce count
        acc.addBounceCnt(curr.getBounceCnt());

        return accTuple;
    }

    public ContentVisitSummary mapContentVisitSummary(String date, Map<String, ProductInfo> prodInfoMap
            , Map<String, ProductDisplayInfo> prodDsplInfoMap
            , Tuple2<ContentVisit, Tuple2<HashSet<String>, HashSet<String>>> tuple
    ) {
        ContentVisit content = tuple._1();
        String productCd = content.getProductCd();
        ProductInfo info = prodInfoMap.get(productCd);
        return new ContentVisitSummary(
                date
                , content.getCurrPageUri()
                , productCd
                , (info == null) ? "" : info.getProductNm()
                , content.getCategoryCdL()
                , content.getCategoryCdM()
                , content.getCategoryCdS()
                , getProductCatgName(prodDsplInfoMap, content.getCategoryCdL())
                , getProductCatgName(prodDsplInfoMap, content.getCategoryCdM())
                , getProductCatgName(prodDsplInfoMap, content.getCategoryCdS())
                , tuple._2()._2().size()  //sessionSet
                , tuple._2()._1().size()  //customerSet
                , content.getNewVisitCnt()
                , content.getPageview_cnt()
                , content.getStayTime()
                , content.getInboundPageCnt()
                , content.getOutboundPageCnt()
                , content.getBounceCnt()
        );
    }

    private String getProductCatgName(Map<String, ProductDisplayInfo> prodDsplInfoMap, String categoryCd) {
        String categoryName = "";
        if (!isEmpty(categoryCd)) {
            ProductDisplayInfo info = prodDsplInfoMap.get(categoryCd);
            if (info != null)
                categoryName = info.getProdCatgNm();
        }
        return categoryName;
    }

    public ContentVisitSummary mapAndSumContentVisitSummary(String date, Map<String, ProductInfo> prodMap
            , Iterable<ContentVisit> contentIter) {
        List<ContentVisit> contentList = new ArrayList<>();
        contentIter.forEach(contentList::add);

        Set<String> customerSet = new HashSet<>();
        ContentVisit first = contentList.get(0);
        String productCd = first.getProductCd();
        ContentVisitSummary contentSumm = createContentVisitSummary(date, prodMap.get(productCd), first);

        for (ContentVisit content : contentList) {
            // visit count
            contentSumm.addVisit_cnt(1);
            // new visit count
            contentSumm.addNew_visitor_cnt(content.getNewVisitCnt());
            // page view count
            contentSumm.addPageview_cnt(content.getPageview_cnt());
            // staying time
            contentSumm.addStay_time(content.getStayTime());
            // page in visit count
            contentSumm.addPagein_visit_cnt(content.getInboundPageCnt());
            // page out visit count
            contentSumm.addPageout_visit_cnt(content.getOutboundPageCnt());
            // bounce count
            contentSumm.addBounce_cnt(content.getBounceCnt());
        }
        // visitor count
        contentSumm.addVisitor_cnt(customerSet.size());

        return contentSumm;
    }

    public ContentVisitSummary createContentVisitSummary(String date, ProductInfo prodInfo
            , ContentVisit content) {

        return new ContentVisitSummary(
                date
                , content.getCurrPageUri()
                , (prodInfo == null) ? "" : prodInfo.getCategoryCdL()
                , (prodInfo == null) ? "" : prodInfo.getCategoryCdM()
                , (prodInfo == null) ? "" : prodInfo.getCategoryCdS()
                , (prodInfo == null) ? "" : prodInfo.getCategoryNmL()
                , (prodInfo == null) ? "" : prodInfo.getCategoryNmM()
                , (prodInfo == null) ? "" : prodInfo.getCategoryNmS()
        );
    }
    // 컨텐츠별방문요약(ContentVisit) //

    // 고객별상품별방문집계(CustProductDaily) //
    public ContentVisit sumCustVisit(ContentVisit acc, ContentVisit curr) {
        acc.addPageview_cnt(curr.getPageview_cnt());
        acc.addStayTime(curr.getStayTime());
        acc.addBasketClickCnt(curr.getBasketClickCnt());
        acc.addZzimClickCnt(curr.getZzimClickCnt());
        return acc;
    }

    public CustProductDaily mapCustProductDaily(String date, Map<String, ProductInfo> prodInfoMap
            , Map<String, ProductDisplayInfo> prodDsplInfoMap, ContentVisit content) {
        String productCd = content.getProductCd();
        ProductInfo info = prodInfoMap.get(productCd);

        return new CustProductDaily(
                date
                , content.getCustId()
                , content.getCookieId()
                , content.getProductCd()
                , (info == null) ? "" : info.getProductNm()
                , content.getCategoryCdL()
                , content.getCategoryCdM()
                , content.getCategoryCdS()
                , ""
                , getProductCatgName(prodDsplInfoMap, content.getCategoryCdL())
                , getProductCatgName(prodDsplInfoMap, content.getCategoryCdM())
                , getProductCatgName(prodDsplInfoMap, content.getCategoryCdS())
                , ""
                , content.getPageview_cnt()
                , content.getStayTime()
                , content.getBasketClickCnt()
                , content.getZzimClickCnt()
        );
    }


//    public Iterable<CustProductDaily> mapCustProductDaily(String date, Map<String, ProductInfo> productInfoMap
//            , Iterable<LogBaseDetail> baseIter) {
//        List<LogBaseDetail> baseList = new ArrayList<>();
//        baseIter.forEach(baseList::add);
//
//        // Sort by page sequence
//        baseList.sort(Comparator.comparingInt(o -> o.getPageSeq()));
//
//        String custId = searchCustId(baseList);
//        String cookie = searchCookie(baseList);
//
//        // Grouping by products
//        Map<String, CustProductDaily> custProdMap = new HashMap<>();
//        for (LogBaseDetail base : baseList) {
//            String productCd = base.getProductCd();
//            if (!isEmpty(productCd)) {
//                CustProductDaily custProduct = custProdMap.get(productCd);
//                if (custProduct == null) {
//                    ProductInfo info = productInfoMap.get(productCd);
//                    custProduct = new CustProductDaily(
//                            date
//                            , custId
//                            , cookie
//                            , productCd
//                            , info.getProductNm()
//                            , info.getCategoryCdL()
//                            , info.getCategoryCdM()
//                            , ""
//                            , info.getCategoryCdS()
//                            , info.getCategoryNmL()
//                            , info.getCategoryNmM()
//                            , info.getCategoryNmS()
//                            , ""
//                            , 1
//                            , base.getStayTime()
//                            , isBasketClick(base.getEventTypeCd()) ? 1 : 0
//                            , isZzimClick(base.getEventTypeCd()) ? 1 : 0
//                    );
//                    custProdMap.put(productCd, custProduct);
//                } else {
//                    custProduct.addProd_detail_pageview_cnt(1);
//                    custProduct.addProd_detail_page_stay_time(base.getStayTime());
//                    if (isBasketClick(base.getEventTypeCd()))
//                        custProduct.addBasket_click_cnt(1);
//                    if (isZzimClick(base.getEventTypeCd()))
//                        custProduct.addZzim_click_cnt(1);
//                }
//            }
//        }
//
//        return custProdMap.values();
//    }

    private boolean isBasketClick(String eventTypeCd) {
        boolean isBascket = false;
        if (!isEmpty(eventTypeCd) && eventTypeCd.equals("31"))
            isBascket = true;
        return isBascket;
    }

    private boolean isZzimClick(String eventTypeCd) {
        boolean isZzim = false;
        if (!isEmpty(eventTypeCd) && eventTypeCd.equals("32"))
            isZzim = true;
        return isZzim;
    }
    // 고객별상품별방문집계(CustProductDaily) //


    // 상품별방문요약(ProductVisitSummary) //
    public ProductVisitSummary mapProductVisitSummary(CustProductDaily product) {
        return new ProductVisitSummary(
                product.getBase_dt()
                , product.getProduct_cd()
                , product.getProduct_nm()
                , product.getProduct_catg_l()
                , product.getProduct_catg_m()
                , product.getProduct_catg_s()
                , product.getProduct_brand_cd()
                , product.getProduct_catg_l_nm()
                , product.getProduct_catg_m_nm()
                , product.getProduct_catg_s_nm()
                , product.getProduct_brand_nm()
                , product.getProd_detail_pageview_cnt()
                , product.getProd_detail_page_stay_time()
                , product.getBasket_click_cnt()
                , product.getZzim_click_cnt()
        );
    }

    public ProductVisitSummary sumProductVisitSummary(ProductVisitSummary acc, ProductVisitSummary curr) {
        acc.addProd_detail_pageview_cnt(curr.getProd_detail_pageview_cnt());
        acc.addProd_detail_page_stay_time(curr.getProd_detail_page_stay_time());
        acc.addBasket_click_cnt(curr.getBasket_click_cnt());
        acc.addZzim_click_cnt(curr.getZzim_click_cnt());
        return acc;
    }
    // 상품별방문요약(ProductVisitSummary) //


    // 시간대 별 방문요약(HourVisitSummary) //
    public Iterable<HourVisitSummary> statHourVisitSummary(String date, Iterable<LogBaseDetail> baseIter) {
        List<LogBaseDetail> baseList = new ArrayList<>();
        baseIter.forEach(baseList::add);

        // Sort
        baseList.sort(Comparator.comparingInt(o -> o.getPageSeq()));

        Map<String, HourVisitSummary> hourVisitMap = new HashMap<>();
        String baseHour = null, baseWeek = null;
        for (LogBaseDetail base : baseList) {
            long timestamp = parseTimeMillis(base.getEventTimestamp());
            // Base week
            if (isEmpty(baseWeek))
                baseWeek = weekF.format(new Date(timestamp));

            // Base hour
            String currHour = hourF.format(new Date(timestamp));
            if (isEmpty(baseHour) || !baseHour.equals(currHour)) {
                baseHour = currHour;
                HourVisitSummary hourVisit = new HourVisitSummary(
                        date
                        , baseHour
                        , baseWeek
                        , 1
                        , 1
                );
                hourVisitMap.put(baseHour, hourVisit);
            } else {
                HourVisitSummary hourVisit = hourVisitMap.get(baseHour);
                hourVisit.addPageview_cnt(1);
            }
        }

        return hourVisitMap.values();
    }
    // 시간대 별 방문요약(HourVisitSummary) //


    private StatisticCount giveStatistics(List<VisitDetail> visitList) {
        StatisticCount stat = new StatisticCount();

        HashSet<String> customerSet = new HashSet<>();
        for (VisitDetail visit : visitList) {
            // Unique customers
            customerSet.add(
                    extractCustomerKey(visit.getCust_id(), visit.getCookie_id())
            );
            // 방문수
            stat.addVisit(1);
            // 회원방문자수
            if (!isEmpty(visit.getCust_id()))
                stat.addCustomer(1);
            // 신규방문자수
            if (visit.getNew_visit_yn().equals("Y"))
                stat.addNewVisitor(1);
            // 페이지뷰수
            stat.addPageView(visit.getPageview_cnt());
            // 순페이지뷰수
            stat.addPage(visit.getPage_cnt());
            // 체류시간
            stat.addStayTime(visit.getStay_time());
            // 반송수
            if (visit.getPageview_cnt() == 1)
                stat.addBounce(1);
            // 구매방문수
            if (visit.getOrder_cnt() > 0)
                stat.addOrder(1);
            // 매출액
            stat.addAmount(visit.getAmount());
        }

        //
        stat.addVisitor(customerSet.size());

        return stat;
    }

    private boolean isNewVisit(String cookieId, String custId, boolean isRegistVisit) {
        boolean isNewVisit = false;
        if (isEmpty(cookieId)) {
            if (isEmpty(custId))
                isNewVisit = true;
            else if (isRegistVisit)
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

    private String extractCustomerKey(String custId, String cookie) {
        // Create group key
        String key = "";
        if (isEmpty(custId)) {
            if (!isEmpty(cookie))
                key = cookie;
        } else {
            key = custId;
        }
        return key;
    }

}