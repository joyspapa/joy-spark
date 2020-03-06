package com.obzen.spark.parser;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.junit.Test;

public class DataConverterTest {

	Pattern pattern = Pattern.compile("\t");
	String append_delimiter = "\t"; // tab
	
	private String makeLog() {
		StringBuilder logSB = new StringBuilder();
		logSB.append("15812346923476");
		logSB.append(append_delimiter);
		logSB.append("http://");
		logSB.append(append_delimiter);
		logSB.append(append_delimiter);
		logSB.append(append_delimiter);
		logSB.append("custid");
		logSB.append(append_delimiter);
		logSB.append("{\"baseProperties\":\"jsonType\"}");
		logSB.append(append_delimiter);
		logSB.append(" ");
		logSB.append(append_delimiter);
		logSB.append("2020ver");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("");
		logSB.append(append_delimiter);
		logSB.append("");
		
		System.out.println("logSB : " + logSB.toString());
		return logSB.toString();
	}
	
	@Test
	public void test() {
		String log = makeLog();
		
		long startT = System.currentTimeMillis();
		
		test02(log);
		
		System.out.println("test elapsed time : " + (System.currentTimeMillis()-startT));
	}
	
	public void test01(String log) {
		String[] val = log.split("\t", -1);
		System.out.println("test01 length : " + val.length);
		
//		for(String s : val) {
//			System.out.println("s : " + s);
//		}
	}
	
	public void test02(String log) {
		String[] val = pattern.split(log, -1);
		System.out.println("test02 length : " + val.length);
		
//		for(String s : val) {
//			System.out.println("s : " + s);
//		}
	}
	
	public void test03(String log) {
		StringTokenizer val = new StringTokenizer(log, append_delimiter);
		System.out.println("test03 length : " + val.countTokens());
		
//		int index = 1;
//		while(val.hasMoreTokens()) {
//		    System.out.println(index + "번째 토큰 : " + val.nextToken());
//		    index++;
//		}
	}
	
	@Test
	public void testDate() {
		//1581408004414
		//1581408004409
		long testLong = 1581408004409L;
		Date testDate = new Date(testLong);
		System.out.println(testDate);
		
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String result = df.format(testLong);
		System.out.println(result);
	}
}
