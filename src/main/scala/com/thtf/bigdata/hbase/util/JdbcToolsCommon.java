package com.thtf.bigdata.hbase.util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * User: ieayoio
 * Date: 18-7-9
 *
 * jdbc工具类接口(Phoenix使用)
 */
public interface JdbcToolsCommon {

    /**
     * 更新或者插入数据
     * @param table 表名称
     * @param map 参数
     * @return 受到影响的行数
     */
    public int upsert(String table, Map<String, Object> map);


    /**
     * 批量更新数据
     * @param table 表名称
     * @param maps 多个参数
     * @return 受影响的行数
     */
    public int[] upsert(String table, Map<String, String>... maps);


    /**
     * 测试查询性能，返回查询时间
     * @param sql sql语句
     * @return 查询时间
     */
    public long queryListForTest(String sql);

    /**
     * 查询数据
     * @param sql sql语句
     * @return 查询结果数据集
     */
    public List<LinkedHashMap<String, Object>> queryList(String sql);


    public void update(String... sql);
}
