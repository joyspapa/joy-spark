package com.obzen.spark.application.batch.cjo.parser;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obzen.spark.application.batch.cjo.pojo.CJO_LOG_STATEFUL;
import com.obzen.spark.application.batch.cjo.pojo.CJO_LOG_STATEFUL_SUM;

public class CjoStatefulScenarioAnalysis implements Serializable {
	private static final Logger logger = LoggerFactory.getLogger(CjoStatefulScenarioAnalysis.class);

	int cacheInsertedCount;
	int visitCount;
	long eventTime;
	int eventOutputCount;
	String eventLastLogTime;

	public CJO_LOG_STATEFUL_SUM analysisSc01(Iterable<CJO_LOG_STATEFUL> sortedStateful) {
		CJO_LOG_STATEFUL_SUM sum = new CJO_LOG_STATEFUL_SUM();
		String sid = "";
		int cnt = 0;
		for (CJO_LOG_STATEFUL stateful : sortedStateful) {
			if (!stateful.getSid().equals("U2cde982185c41044e76846916064947e91ec1579755794")) {
				return null;
			}
			if (cnt == 0) {
				sid = stateful.getSid();
			}
			conditionSc01(stateful);
			cnt++;
		}

		if (logger.isDebugEnabled()) {
			logger.info("[시나리오 1] sid : {} , 이벤트 발생 총 건수 : {} , 캐시 insert 건수 : {} ", sid, eventOutputCount,
					cacheInsertedCount);
		}
		sum.setEventOutputCount(eventOutputCount);
		sum.setCacheInsertedCount(cacheInsertedCount);
		sum.setSid(sid);
		return sum;
	}

	public CJO_LOG_STATEFUL_SUM analysisSc02(Iterable<CJO_LOG_STATEFUL> sortedStateful) {
		CJO_LOG_STATEFUL_SUM sum = new CJO_LOG_STATEFUL_SUM();
		String sid = "";
		int cnt = 0;
		for (CJO_LOG_STATEFUL stateful : sortedStateful) {
			if (!stateful.getSid().equals("U2cde982185c41044e76846916064947e91ec1579755794")) {
				return null;
			}
			if (cnt == 0) {
				sid = stateful.getSid();
			}
			conditionSc02(stateful);
			cnt++;
		}

		if (logger.isDebugEnabled()) {
			logger.info("[시나리오 2] sid : {} , 이벤트 발생 총 건수 : {} , 캐시 insert 건수 : {} ", sid, eventOutputCount,
					cacheInsertedCount);
		}
		sum.setEventOutputCount(eventOutputCount);
		sum.setCacheInsertedCount(cacheInsertedCount);
		sum.setSid(sid);
		return sum;
	}

	public CJO_LOG_STATEFUL_SUM analysisSc03(Iterable<CJO_LOG_STATEFUL> sortedStateful) {
		CJO_LOG_STATEFUL_SUM sum = new CJO_LOG_STATEFUL_SUM();
		String sid = "";
		int cnt = 0;
		for (CJO_LOG_STATEFUL stateful : sortedStateful) {
			if (!stateful.getSid().equals("U2cde982185c41044e76846916064947e91ec1579755794")) {
				return null;
			}
			if (cnt == 0) {
				sid = stateful.getSid();
			}
			conditionSc03(stateful);
			cnt++;
		}

		if (logger.isDebugEnabled()) {
			logger.info("[시나리오 3] sid : {} , 이벤트 발생 총 건수 : {} , 캐시 insert 건수 : {} ", sid, eventOutputCount,
					cacheInsertedCount);
		}
		sum.setEventOutputCount(eventOutputCount);
		sum.setCacheInsertedCount(cacheInsertedCount);
		sum.setSid(sid);
		return sum;
	}

	public CJO_LOG_STATEFUL_SUM analysisSc04(Iterable<CJO_LOG_STATEFUL> sortedStateful) {
		CJO_LOG_STATEFUL_SUM sum = new CJO_LOG_STATEFUL_SUM();
		String sid = "";
		int cnt = 0;
		for (CJO_LOG_STATEFUL stateful : sortedStateful) {
			if (!stateful.getSid().equals("U2cde982185c41044e76846916064947e91ec1579755794")) {
				return null;
			}
			if (cnt == 0) {
				sid = stateful.getSid();
			}
			conditionSc04(stateful);
			cnt++;
		}

		if (logger.isDebugEnabled()) {
			logger.info("[시나리오 4] sid : {} , 이벤트 발생 총 건수 : {} , 캐시 insert 건수 : {} ", sid, eventOutputCount,
					cacheInsertedCount);
		}
		sum.setEventOutputCount(eventOutputCount);
		sum.setCacheInsertedCount(cacheInsertedCount);
		sum.setSid(sid);
		return sum;
	}
	
	public void sc01(Iterable<CJO_LOG_STATEFUL> sortedStateful) {

		for (CJO_LOG_STATEFUL stateful : sortedStateful) {
			conditionSc01(stateful);
		}
		if (logger.isDebugEnabled()) {
			logger.info("[시나리오 1] 이벤트 발생 총 건수 : {} , 캐시 insert 건수 : {} ", eventOutputCount, cacheInsertedCount);
		}
	}

	private void conditionSc01(CJO_LOG_STATEFUL stateful) {
		long interval = (stateful.getEventTime() - eventTime) / 1000;
		if (eventTime != 0 && interval >= 17) {
			if (visitCount > 2) {
				if (logger.isDebugEnabled()) {
					logger.debug("[시나리오 1] 이벤트 발생 : visitCount :{} , log_time : {}, interval : {}", visitCount,
							eventLastLogTime, interval);
				}
				eventOutputCount++;
			}
			visitCount = 0;
		}

		eventLastLogTime = stateful.getLog_time();
		eventTime = stateful.getEventTime();

		if (stateful.getCur_pg_info1().equals("/item") || stateful.getCur_pg_info1().equals("/mocode")
				|| stateful.getCur_pg_info1().equals("/cart")
				|| (stateful.getCur_pg_info1().equals("/search") && stateful.getCur_pg_info2().equals("/searchAllList"))
				|| (stateful.getCur_pg_info1().equals("/myzone") && stateful.getCur_pg_info2().equals("/zzimList"))) {

			visitCount++;
			cacheInsertedCount++;
			//logger.info("이벤트 저장 : visitCount :{} , log_time : {}", visitCount, stateful.getLog_time());

		} else if ((stateful.getCur_pg_info1().equals("/order") && stateful.getCur_pg_info2().equals("/approval"))
				|| (stateful.getCur_pg_info1().equals("/order") && stateful.getCur_pg_info2().equals("/end"))) {
			//logger.info("이벤트 초기 : visitCount :{} , log_time : {}", visitCount, stateful.getLog_time());
			visitCount = 0;
			eventTime = 0;
		}
	}

	private void conditionSc02(CJO_LOG_STATEFUL stateful) {
		long interval = (stateful.getEventTime() - eventTime) / 1000;
		if (eventTime != 0 && interval >= 300) {
			if (visitCount > 2) {
				if (logger.isDebugEnabled()) {
					logger.debug("[시나리오 2] 이벤트 발생 : visitCount :{} , log_time : {}, interval : {}", visitCount,
							eventLastLogTime, interval);
				}
				eventOutputCount++;
			}
			visitCount = 0;
		}

		eventLastLogTime = stateful.getLog_time();
		eventTime = stateful.getEventTime();

		if ((stateful.getCur_pg_info1().equals("/myzone") && stateful.getCur_pg_info2().equals("/coupinList"))
				|| (stateful.getCur_pg_info1().equals("/myzone") && stateful.getCur_pg_info2().equals("/gradeBenefit"))
				|| (stateful.getCur_pg_info1().equals("/myzone") && stateful.getCur_pg_info2().equals("/expectedGrade"))
				|| (stateful.getCur_pg_info1().equals("/myzone") && stateful.getCur_pg_info2().equals("/depositList"))
				|| (stateful.getCur_pg_info1().equals("/myzone")
						&& stateful.getCur_pg_info2().equals("/cjonePointList"))
				|| (stateful.getCur_pg_info1().equals("/order") && stateful.getCur_pg_info2().equals("/sheet"))) {

			visitCount++;
			cacheInsertedCount++;
			//logger.info("이벤트 저장 : visitCount :{} , log_time : {}", visitCount, stateful.getLog_time());

		} else if ((stateful.getCur_pg_info1().equals("/order") && stateful.getCur_pg_info2().equals("/approval"))
				|| (stateful.getCur_pg_info1().equals("/order") && stateful.getCur_pg_info2().equals("/end"))) {
			//logger.info("이벤트 초기 : visitCount :{} , log_time : {}", visitCount, stateful.getLog_time());
			visitCount = 0;
			eventTime = 0;
		}
	}

	private void conditionSc03(CJO_LOG_STATEFUL stateful) {
		long interval = (stateful.getEventTime() - eventTime) / 1000;
		if (eventTime != 0 && interval >= 300) {
			if (visitCount > 2) {
				if (logger.isDebugEnabled()) {
					logger.debug("[시나리오 3] 이벤트 발생 : visitCount :{} , log_time : {}, interval : {}", visitCount,
							eventLastLogTime, interval);
				}
				eventOutputCount++;
			}
			visitCount = 0;
		}

		eventLastLogTime = stateful.getLog_time();
		eventTime = stateful.getEventTime();

		if ((stateful.getCust_cd().length() == 0 && stateful.getInfl_cd().length() > 0)) {

			visitCount++;
			cacheInsertedCount++;
			//logger.info("이벤트 저장 : visitCount :{} , log_time : {}", visitCount, stateful.getLog_time());

		} else if ((stateful.getCur_pg_info1().equals("/order") && stateful.getCur_pg_info2().equals("/approval"))
				|| (stateful.getCur_pg_info1().equals("/order") && stateful.getCur_pg_info2().equals("/end"))) {
			//logger.info("이벤트 초기 : visitCount :{} , log_time : {}", visitCount, stateful.getLog_time());
			visitCount = 0;
			eventTime = 0;
		}
	}
	
	private void conditionSc04(CJO_LOG_STATEFUL stateful) {
		long interval = (stateful.getEventTime() - eventTime) / 1000;
		if (eventTime != 0 && interval >= 300) {
			if (visitCount > 2) {
				if (logger.isDebugEnabled()) {
					logger.debug("[시나리오 4] 이벤트 발생 : visitCount :{} , log_time : {}, interval : {}", visitCount,
							eventLastLogTime, interval);
				}
				eventOutputCount++;
			}
			visitCount = 0;
		}

		eventLastLogTime = stateful.getLog_time();
		eventTime = stateful.getEventTime();

		if ((stateful.getRef_pg_info1().equals("/exhibition") && stateful.getRef_pg_info2().equals("/exhibitionDetail"))
				&& ((stateful.getCur_pg_info1().equals("/item") || stateful.getCur_pg_info2().equals("/mocode")))) {

			visitCount++;
			cacheInsertedCount++;
			//logger.info("이벤트 저장 : visitCount :{} , log_time : {}", visitCount, stateful.getLog_time());

		} else if ((stateful.getCur_pg_info1().equals("/order") && stateful.getCur_pg_info2().equals("/approval"))
				|| (stateful.getCur_pg_info1().equals("/order") && stateful.getCur_pg_info2().equals("/end"))) {
			//logger.info("이벤트 초기 : visitCount :{} , log_time : {}", visitCount, stateful.getLog_time());
			visitCount = 0;
			eventTime = 0;
		}
	}

	private void printInfo(CJO_LOG_STATEFUL stateful) {
		logger.info(" stateful : {} , {} , {} , {} , {} , {}, {}", stateful.getLog_time(), stateful.getCust_cd(),
				stateful.getCur_pg_info1(), stateful.getCur_pg_info2(), stateful.getRef_pg_info1(),
				stateful.getRef_pg_info2(), stateful.getSid());
	}

	private void conditionTest(CJO_LOG_STATEFUL stateful) {
		long interval = (stateful.getEventTime() - eventTime) / 1000;

		if (eventTime != 0 && interval >= 17) {
			if (visitCount > 2) {
				logger.info("이벤트 발생 : visitCount :{} , log_time : {}, interval : {}", visitCount, eventLastLogTime,
						interval);
				visitCount = 0;
				//eventTime = 0;
				//			} else if (eventTime != 0 && interval >= 1800) {
				//				visitCount = 0;
				//				eventTime = 0;
				//				logger.info("이벤트 1800 : visitCount :{} , log_time : {}, interval : {}", visitCount, stateful.getLog_time(),interval);
			} else {
				visitCount = 0;
				//eventTime = 0;
				//logger.info("이벤트 else : visitCount :{} , log_time : {}, interval : {}", visitCount, stateful.getLog_time(),interval);
			}
		}
		//		else {
		//			logger.info("이벤트 eventTime != 0 : visitCount :{} , log_time : {}, interval : {}", visitCount, stateful.getLog_time(),interval);
		//		}

		eventLastLogTime = stateful.getLog_time();
		eventTime = stateful.getEventTime();

		//logger.info("이벤트 interval : visitCount :{} , log_time : {}, interval : {}", visitCount, stateful.getLog_time(),stateful.getEventTime() - eventTime);

		if (stateful.getCur_pg_info1().equals("/item") || stateful.getCur_pg_info1().equals("/mocode")
				|| stateful.getCur_pg_info1().equals("/cart")
				|| (stateful.getCur_pg_info1().equals("/search") && stateful.getCur_pg_info2().equals("/searchAllList"))
				|| (stateful.getCur_pg_info1().equals("/myzone") && stateful.getCur_pg_info2().equals("/zzimList"))) {

			visitCount++;
			logger.info("이벤트 저장 : visitCount :{} , log_time : {}", visitCount, stateful.getLog_time());
		} else if ((stateful.getCur_pg_info1().equals("/order") && stateful.getCur_pg_info2().equals("/approval"))
				|| (stateful.getCur_pg_info1().equals("/order") && stateful.getCur_pg_info2().equals("/end"))) {

			logger.info("이벤트 초기 : visitCount :{} , log_time : {}", visitCount, stateful.getLog_time());
			visitCount = 0;
			eventTime = 0;
		}
	}
}
