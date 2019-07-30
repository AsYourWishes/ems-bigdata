package com.thtf.bigdata.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author zhangqiang
 * create date:2014-8-28
 */
public class DateUtil {

	public final static String yMdHms="yyyyMMddHHmmss";
	public final static String y_M_d_H_m_s="yyyy-MM-dd HH:mm:ss";
	/**
	 * yyyyMMddHHmmss
	 * @author zhangqiang
	 * create date:2014-8-28
	 */
	public static String getCurrentDateToStr(String format){
		SimpleDateFormat df=new SimpleDateFormat(format);
		return df.format(new Date());
	}
	
	public static String getDateStr(Date date){
		SimpleDateFormat df=new SimpleDateFormat(y_M_d_H_m_s);
		return df.format(date);
	}
	public static Date string2Date(String dateStr,String pattern){
		DateFormat df = new SimpleDateFormat(pattern);
		Date date = null;
		try {
			date = df.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
	
	public static String date2String(Date date,String pattern){
		SimpleDateFormat df=new SimpleDateFormat(pattern);
		return df.format(date);
	}
	
	public static Date addMonth(Date date,int i) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.MONTH, i);    
		return c.getTime();
	}
	public static Date addYear(Date date,int i) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.YEAR, i);    
		return c.getTime();
	}
}
