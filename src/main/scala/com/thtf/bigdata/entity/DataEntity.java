package com.thtf.bigdata.entity;

import java.util.ArrayList;
import java.util.List;

import com.thtf.bigdata.common.Contant;
import com.thtf.bigdata.entity.CommonXmlEntity;

/**
 * @author zhangqiang
 * create date:2014-8-25
 */
public class DataEntity extends CommonXmlEntity{

	private String sequence;//数据序号
	private String parser;//采集器是否解析过 
	private String time;//yyyyMMddHHmmss
	private List<MeterItemEntity> meterItems=new ArrayList<MeterItemEntity>();//计量设备集合
	
	public DataEntity() {
		dataType=Contant.DATA;
		operation=Contant.CONTINUOUS_ACK;
	}
	
	public String getSequence() {
		return sequence;
	}
	public void setSequence(String sequence) {
		this.sequence = sequence;
	}
	public String getParser() {
		return parser;
	}
	public void setParser(String parser) {
		this.parser = parser;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public List<MeterItemEntity> getMeterItems() {
		return meterItems;
	}
	public void setMeterItems(List<MeterItemEntity> meterItems) {
		this.meterItems = meterItems;
	}
	@Override
	public String toString() {
		return "DataEntity [buildingId=" + buildingId + ", gatewayId="
				+ gatewayId + ", operation=" + operation + ", sequence="
				+ sequence + ", parser=" + parser + ", time=" + time
				+ ", meterItems=" + meterItems + "]";
	}
}
