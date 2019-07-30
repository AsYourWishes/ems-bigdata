package com.thtf.bigdata.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * 计量设备
 * @author zhangqiang
 * create date:2014-8-25
 */
public class MeterItemEntity{

	private String id;
	private String meterTime;//yyyyMMddHHmmss  本地采集器协议里每个电表一个时间
	private List<ItemFunctionEntity> itemFunctions=new ArrayList<ItemFunctionEntity>();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<ItemFunctionEntity> getItemFunctions() {
		return itemFunctions;
	}

	public void setItemFunctions(List<ItemFunctionEntity> itemFunctions) {
		this.itemFunctions = itemFunctions;
	}
	public String getMeterTime() {
		return meterTime;
	}

	public void setMeterTime(String meterTime) {
		this.meterTime = meterTime;
	}

	@Override
	public String toString() {
		return "MeterItemEntity [id=" + id + ", meterTime=" + meterTime
				+ ", itemFunctions=" + itemFunctions + "]";
	}
}
