package com.thtf.bigdata.entity;


/**
 * @author zhangqiang
 * create date:2014-8-28
 */
public class CommonXmlEntity{

	protected String buildingId;
	protected String gatewayId;
	protected String operation;
	protected String dataType;

	public String getBuildingId() {
		return buildingId;
	}

	public void setBuildingId(String buildingId) {
		this.buildingId = buildingId;
	}

	public String getGatewayId() {
		return gatewayId;
	}

	public void setGatewayId(String gatewayId) {
		this.gatewayId = gatewayId;
	}

	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	/**
	 * 获取采集器编号
	 * @return
	 * @author zhangqiang
	 * create date:2014-8-28
	 */
	public String getAllCollectorCode(){
		return buildingId+"_"+gatewayId;
	}
}
