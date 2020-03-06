package com.obzen.spark.parser;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

public class TimestampParserTest {

	SimpleDateFormat timestampDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	SimpleDateFormat timestampDateFormat2 = new SimpleDateFormat("yyyyMMddHHmmssSSSSS");

	@Test
	public void test() {
		try {
			DateFormat nanoDfHNT = new SimpleDateFormat("yyyyMMddHHmmssSSSSSSSSS");
			String str = nanoDfHNT.format(new Date(System.currentTimeMillis()));
			//str = "2661102706473423684";
			System.out.println("Date : " + str);
			
			Date date = timestampDateFormat.parse(str);
			System.out.println("time : " + date.getTime());
			
			Date date2 = timestampDateFormat2.parse(str);
			System.out.println("time : " + date2.getTime());
			
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("end");
	}
}
