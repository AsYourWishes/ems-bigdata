package com.thtf.bigdata.entity;



/**
 * 封装某连接的采集器信息
 * @author zhangqiang
 * create date:2014-8-27
 */
public class ChannelHandlerEntity{

	/**
	 * 建筑id
	 */
	private Long unitId;
	/**
	 * 建筑编号
	 */
	private String buildingId;
	/**
	 * 采集器编号
	 */
	private String gatewayId;
	/**
	 * 离线计时器
	 */
	private Integer offLineTime;
	
	/**
	 * 采集周期
	 */
	private Integer period;
	
	/**
	 * 采集器更新周期配置时间
	 */
	private String updatePeriodTime;
	
	public ChannelHandlerEntity() {
	}
	public ChannelHandlerEntity(String buildingId, String gatewayId) {
		this(buildingId,gatewayId,0);
	}
	public ChannelHandlerEntity(String buildingId, String gatewayId,
			Integer offLineTime){
		super();
		this.buildingId = buildingId;
		this.gatewayId = gatewayId;
		this.offLineTime = offLineTime;
	}
	public Long getUnitId() {
		return unitId;
	}
	public void setUnitId(Long unitId) {
		this.unitId = unitId;
	}
	
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
	public Integer getPeriod() {
		return period;
	}
	public void setPeriod(Integer period) {
		this.period = period;
	}
	public String getUpdatePeriodTime() {
		return updatePeriodTime;
	}
	public void setUpdatePeriodTime(String updatePeriodTime) {
		this.updatePeriodTime = updatePeriodTime;
	}
	public Integer getOffLineTime() {
		return offLineTime;
	}
	public void setOffLineTime(Integer offLineTime) {
		this.offLineTime = offLineTime;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((buildingId == null) ? 0 : buildingId.hashCode());
		result = prime * result
				+ ((gatewayId == null) ? 0 : gatewayId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ChannelHandlerEntity other = (ChannelHandlerEntity) obj;
		if (buildingId == null) {
			if (other.buildingId != null)
				return false;
		} else if (!buildingId.equals(other.buildingId))
			return false;
		if (gatewayId == null) {
			if (other.gatewayId != null)
				return false;
		} else if (!gatewayId.equals(other.gatewayId))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "ChannelHandlerEntity [unitId=" + unitId + ", buildingId="
				+ buildingId + ", gatewayId=" + gatewayId + ", offLineTime="
				+ offLineTime + ", period=" + period + ", updatePeriodTime="
				+ updatePeriodTime + "]";
	}
}
