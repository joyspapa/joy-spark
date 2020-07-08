package com.obzen.spark.parser;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

public class TimestampParserTest {

	SimpleDateFormat timestampDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	SimpleDateFormat timestampDateFormat2 = new SimpleDateFormat("yyyyMMddHHmmssSSSSS");
	SimpleDateFormat timestampDateFormat3 = new SimpleDateFormat("");
	
	@Test
	public void test() {
		try {
			DateFormat nanoDfHNT = new SimpleDateFormat("yyyyMMddHHmmssSSSSSS");
			String str = nanoDfHNT.format(new Date(System.currentTimeMillis()));
			//str = "2661102706473423684";
			str = "20200429182753915636";
			       //20200429183528000000966
			       //20200429183651000552
			System.out.println("Date : " + str);
			
			Date date = timestampDateFormat.parse(str);
			timestampDateFormat.setLenient(false);
			System.out.println("timestampDateFormat : " + date.getTime());
			
			Date date2 = timestampDateFormat2.parse(str);
			timestampDateFormat2.setLenient(false);
			System.out.println("timestampDateFormat2 : " + date2.getTime());
			
			System.out.println("timestampDateFormat2.toPattern : '" + timestampDateFormat2.toPattern()+"'");
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("end");
	}
}
