package com.thtf.bigdata.entity;


/**
 * @author zhangqiang
 * create date:2014-8-26
 */
public class ItemFunctionEntity{

	private Long id;
	private String coding;
	private String error;
	private String value;
	private String unit;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getCoding() {
		return coding;
	}
	public void setCoding(String coding) {
		this.coding = coding;
	}
	public String getError() {
		return error;
	}
	public void setError(String error) {
		this.error = error;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getUnit() {
		return unit;
	}
	public void setUnit(String unit) {
		this.unit = unit;
	}
	@Override
	public String toString() {
		return "ItemFunctionEntity [id=" + id + ", coding=" + coding
				+ ", error=" + error + ", value=" + value + ", unit=" + unit
				+ "]";
	}
}
