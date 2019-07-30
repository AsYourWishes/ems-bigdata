package com.thtf.bigdata.common;
/**
 * 常量类
 * @author zhangqiang
 * create date:2014-8-27
 */
public class Contant {

	/**
	 * 第三方数据包类型常量
	 */
	public static final String DATA="data";//上传数据包
	public static final String ID_VALIDATE="id_validate";//身份验证数据包
	public static final String HEART_BEAT="heart_beat";//心跳/校时数据包
	public static final String CONFIG="config";//配置信息数据包
	
	/**
	 * 本地数据包类型常量
	 */
	public static final String LOCAL_VERIFY="verify";//身份验证数据包
	public static final String METER="meter";//上传数据包
	
	/**
	 * 数据包的操作类型常量
	 */
	public static final String REQUEST="request";//request:采集器请求身份验证（该数据包为采集器发送给服务器）
	public static final String RESULT="result";//result:服务器发送验证结果，result子元素有效（该数据包为服务器发送给采集器）
	public static final String NOTIFY="notify";//notify:采集器定期给服务器发送存活通知，不需要子元素(每一分钟发一次，五分钟不发设为离线，并关闭链接) （该数据包为采集器发送给服务器）
	public static final String TIME="time";//time:服务器在收到存活通知后发送授时信息，此时子元素time有效,采集器的时间需要以服务器时间为准（该数据包为服务器发送给采集器）
	public static final String REPORT="report";//report:采集器定时上报的能耗数据（该数据包为采集器发送给服务器）
	public static final String CONTINUOUS_ACK="continuous_ack";//continuous_ack:全部续传数据包接收完成后，服务器对上传数据的应答，不需要子元素（该数据包为服务器发送给采集器）
	public static final String PERIOD="period";// period:表示服务器对采集器采集周期的配置,period子元素有效（该数据包为服务器发送给采集器）
	public static final String PERIOD_ACK="period_ack";//period_ack:表示采集器对服务器采集周期配置信息的应答（该数据包为采集器发送给服务器）
	
	/**
	 * 本地服务需要用到的
	 * 数据包的操作类型常量
	 */
	public static final String SEQUENCE="sequence";//数据中心向数据采集器发送一个随机序列（该数据包为服务器发送给采集器）
	public static final String MD5="md5";//发送MD5码（该数据包为采集器发送给服务器）
	
	/**
	 * 验证通不通过
	 */
	public final static String PASS="pass";//通过
	public final static String FAIL="fail";//未通过
	
	public final static String HOUR="hour";//小时
	public final static String DAY="day";//天
	public final static String MONTH="month";//月
}
