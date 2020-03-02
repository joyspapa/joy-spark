package com.obzen.spark.application.batch.cjo.parser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.apache.spark.sql.Row;

import com.obzen.spark.application.batch.cjo.pojo.CJO_LOG_BASE;
import com.obzen.spark.application.batch.cjo.pojo.CJO_LOG_STATEFUL;

/**
 * Created by inykang on 17. 4. 28.
 */
public class CjoBaseLogParser implements Serializable {
    
    public CJO_LOG_STATEFUL mapCJO_LOG_BASE(Row row) {
    	    	
    	/*
    	 * 조건
    	 */
        return new CJO_LOG_STATEFUL(
        		Objects.toString(row.getAs("log_time"), "")
        		, Objects.toString(row.getAs("cust_age"), "")
        		, Objects.toString(row.getAs("app_cd"), "")
        		, Objects.toString(row.getAs("brand_cd"), "")
        		, Objects.toString(row.getAs("chn_cd"), "")
        		, Objects.toString(row.getAs("click_cd"), "")
        		, Objects.toString(row.getAs("cur_url_chn_tp"), "")
        		, Objects.toString(row.getAs("cur_pg_info1"), "")
        		, Objects.toString(row.getAs("cur_pg_info2"), "")
        		, Objects.toString(row.getAs("date_wday"), "")
        		, Objects.toString(row.getAs("cust_empl_fl"), "")
        		, Objects.toString(row.getAs("cust_grade"), "")
        		, Objects.toString(row.getAs("infl_cd"), "")
        		, Objects.toString(row.getAs("infl_grp_cd"), "")
        		, Objects.toString(row.getAs("intl_pgm_cd"), "")
        		, Objects.toString(row.getAs("md_cd"), "")
        		, Objects.toString(row.getAs("udid"), "")
        		, Objects.toString(row.getAs("pgm_cd"), "")
        		, Objects.toString(row.getAs("hometab_pic"), "")
        		, Objects.toString(row.getAs("ref_pg_info1"), "")
        		, Objects.toString(row.getAs("ref_pg_info2"), "")
        		, Objects.toString(row.getAs("search_result"), "")
        		, Objects.toString(row.getAs("search_word"), "")
        		, Objects.toString(row.getAs("cust_gender"), "")
        		, Objects.toString(row.getAs("shop_cd"), "")
        		, Objects.toString(row.getAs("sid"), "")
        		, Objects.toString(row.getAs("std_cate_cd"), "")
        		, Objects.toString(row.getAs("uid"), "")
        		, Objects.toString(row.getAs("cust_cd"), "")
        		, Objects.toString(row.getAs("cust_id"), "")
        		, Objects.toString(row.getAs("cust_visit_fl"), "")
        		, Objects.toString(row.getAs("visit_login_fl"), "")
        		, Objects.toString(row.getAs("visit_pg_seq"), "")
        		, Objects.toString(row.getAs("vipmall_clk_yn"), "")
        		, Objects.toString(row.getAs("vipclub_clk_yn"), "")
        );
    }
    
    public CJO_LOG_STATEFUL mergeVisitCount(CJO_LOG_STATEFUL before, CJO_LOG_STATEFUL curr) {
//        if (isEmpty(curr.getCust_id())) {
//        	before.setCust_id(curr.getCust_id());
//        	before.setSex(curr.getSex());
//        	before.setAge(curr.getAge());
//        }
    	before.setVisitCnt(before.getVisitCnt()+1);
        return before;
    }
    
    public Iterable<CJO_LOG_STATEFUL> sortLogTime(Iterable<CJO_LOG_STATEFUL> statefulIter) {
    	// Sort and set baseDetails
        List<CJO_LOG_STATEFUL> sortEventTimeList = new ArrayList<>();
        statefulIter.forEach(sortEventTimeList::add);

        // Sort by timestamp
        sortEventTimeList.sort(Comparator.comparing(CJO_LOG_STATEFUL::getEventTime));
        
    	return sortEventTimeList;
    }
}