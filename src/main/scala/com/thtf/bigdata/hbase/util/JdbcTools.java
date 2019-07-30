package com.thtf.bigdata.hbase.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.alibaba.druid.pool.DruidDataSource;
import com.thtf.bigdata.util.PropertiesUtils;

/**
 * User: ieayoio
 * Date: 18-6-20
 */
public class JdbcTools implements JdbcToolsCommon {
    private JdbcTemplate jdbcTemplate;
    private String nameSpace = "";
    public JdbcTemplate getJdbcTemplate(int flag, String nameSpace) {
//        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        // 切换阿里数据源，UPSERT可以生效了
        DruidDataSource dataSource = new DruidDataSource();
        switch (flag) {
            case 0:
                dataSource.setDriverClassName(PropertiesUtils.getPropertiesByKey("driver.mysql"));
                dataSource.setUrl(PropertiesUtils.getPropertiesByKey("url.mysql"));
                dataSource.setUsername(PropertiesUtils.getPropertiesByKey("user.mysql"));
                dataSource.setPassword(PropertiesUtils.getPropertiesByKey("password.mysql"));
                dataSource.setPoolPreparedStatements(false);
                break;
            case 1:
                dataSource.setDriverClassName(JdbcUtils.PHOENIX_DRIVER_CLASS_NAME);
                dataSource.setUrl(JdbcUtils.PHOENIX_URL);
                dataSource.setPoolPreparedStatements(true);
                if (nameSpace != null) {
                    Properties prop = new Properties();
                    prop.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
                    if (!"".equals(nameSpace)) {
                        prop.setProperty("phoenix.connection.schema", "\"" + nameSpace +
                                "\"");
                    }
                    dataSource.setConnectProperties(prop);
                }
                break;
            case 2:
                dataSource.setDriverClassName(PropertiesUtils.getPropertiesByKey("driver.sqlserver"));
                dataSource.setUrl(PropertiesUtils.getPropertiesByKey("url.sqlserver"));
                dataSource.setUsername(PropertiesUtils.getPropertiesByKey("user.sqlserver"));
                dataSource.setPassword(PropertiesUtils.getPropertiesByKey("password.sqlserver"));
                dataSource.setPoolPreparedStatements(false);
                break;
            default:
                break;
        }

        dataSource.setTestWhileIdle(false);
        dataSource.setMaxActive(300);
        dataSource.setInitialSize(1);
        dataSource.setMaxWait(1800000);
        dataSource.setMinIdle(1);
        dataSource.setTimeBetweenEvictionRunsMillis(600000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setMaxPoolPreparedStatementPerConnectionSize(50);


//        dataSource.setUsername(USERNAME);
//        dataSource.setPassword(PASSWORD);
        return new JdbcTemplate(dataSource);
    }

    private static JdbcTools jdbcMysql;
    private static JdbcTools jdbcSqlServer;
    private static JdbcTools jdbcPhoenixNameSpace;
    private static JdbcTools jdbcPhoenixNoNameSpace;
    private static JdbcTools jdbcPhoenixNonuseNameSpace;
    /**
     * 实例化对象
     *
     * @param flag:数据库标识: 0:mysql, 1:phoenix, 2:sqlServer
     * @return
     */
    public static JdbcTools getInstance(int flag, String nameSpace) {
        switch (flag) {
            case 0:
                if (jdbcMysql == null) {
                    jdbcMysql = new JdbcTools(flag, nameSpace);
                }
                return jdbcMysql;
            case 1:
                if (nameSpace == null){
                    if (jdbcPhoenixNonuseNameSpace == null) {
                        jdbcPhoenixNonuseNameSpace = new JdbcTools(flag, nameSpace);
                    }
                    return jdbcPhoenixNonuseNameSpace;
                }
                else if ("".equals(nameSpace)) {
                    if (jdbcPhoenixNoNameSpace == null) {
                        jdbcPhoenixNoNameSpace = new JdbcTools(flag, nameSpace);
                    }
                    return jdbcPhoenixNoNameSpace;
                } else {
                    if (jdbcPhoenixNameSpace == null || !jdbcPhoenixNameSpace.nameSpace.equals(nameSpace)) {
                        jdbcPhoenixNameSpace = new JdbcTools(flag, nameSpace);
                    }
                    return jdbcPhoenixNameSpace;
                }
            case 2:
                if (jdbcSqlServer == null) {
                    jdbcSqlServer = new JdbcTools(flag, nameSpace);
                }
                return jdbcSqlServer;
            default:
                return null;
        }
    }

    public static JdbcTools getInstance(int flag) {
        return getInstance(flag, null);
    }

    private JdbcTools(int flag, String nameSpace) {
        this.nameSpace = nameSpace;
        jdbcTemplate = getJdbcTemplate(flag, nameSpace);
    }

    // 根据map拼接sql语句(通过拼接sql语句形式)
    @Deprecated
    private String upsertSQLJoin2(String table, Map<String, String> map) {
        StringBuffer sqlbuffer = new StringBuffer("UPSERT INTO ");
        sqlbuffer.append("\"").append(table).append("\"");

        StringJoiner keyJoiner = new StringJoiner(",");
        StringJoiner valueJoiner = new StringJoiner(",");

        for (String key : map.keySet()) {
            keyJoiner.add("\"" + key + "\"");
            valueJoiner.add("'" + map.getOrDefault(key, null) + "'");
        }
        sqlbuffer.append(" (").append(keyJoiner.toString()).append(") VALUES ( ").append(valueJoiner.toString()).append(" )");
        return sqlbuffer.toString();
    }

    // 根据map拼接sql语句(通过参数形式)
    private String upsertSQLJoin(String table, Map<String, Object> map, List<Object> params) {
        StringBuffer sqlbuffer = new StringBuffer("UPSERT INTO ");
        sqlbuffer.append("\"").append(table).append("\"");

        StringJoiner keyJoiner = new StringJoiner(",");
//        StringJoiner valueJoiner = new StringJoiner(",");
        StringJoiner paramsJoiner = new StringJoiner(",");

        for (String key : map.keySet()) {
            keyJoiner.add("\"" + key + "\"");
            paramsJoiner.add("?");
            params.add(map.getOrDefault(key, null));
//            valueJoiner.add("'" + map.getOrDefault(key, null) + "'");
        }
        sqlbuffer.append(" (").append(keyJoiner.toString()).append(") VALUES ( ").append(paramsJoiner.toString()).append(" )");
        return sqlbuffer.toString();
    }


    /**
     * 返回受影响行数
     *
     * @param table 表名
     * @param map   插入的数据map
     * @return
     */
    @Override
    public int upsert(String table, Map<String, Object> map) {

        List<Object> params = new ArrayList<>();
        String sql = upsertSQLJoin(table, map, params);
        return jdbcTemplate.update(sql, params.toArray(new Object[0]));
    }


    // 批量upsert
    @Override
    public int[] upsert(String table, Map<String, String>... maps) {

        if (maps == null || maps.length == 0) {
            return null;
        }

        List<String> sqlList = new ArrayList<>();

        for (Map<String, String> map : maps) {
            String sql = upsertSQLJoin2(table, map);
            sqlList.add(sql);
        }

        return jdbcTemplate.batchUpdate(sqlList.toArray(new String[0]));

    }

    public boolean hasTable(String tableName) {
        boolean hasTab = false;
        List<LinkedHashMap<String, Object>> listmap = queryList("select ");
        return hasTab;
    }

    @Override
    public void update(String... sql) {
        if (sql == null || sql.length == 0) {
            return;
        }

        if (sql.length == 1) {
            jdbcTemplate.update(sql[0]);
        } else {
            jdbcTemplate.batchUpdate(sql);
        }
    }

    @Override
    public long queryListForTest(String sql) {
        long l = System.currentTimeMillis();
        jdbcTemplate.query(sql, (resultSet, i) -> null);
        return System.currentTimeMillis() - l;
    }

    // 查询sql语句，返回map集合
    @Override
    public List<LinkedHashMap<String, Object>> queryList(String sql) {
        List<LinkedHashMap<String, Object>> query = jdbcTemplate.query(sql, new RowMapper<LinkedHashMap<String, Object>>() {
            @Override
            public LinkedHashMap<String, Object> mapRow(ResultSet resultSet, int i) throws SQLException {

                return JdbcUtils.getObjectFromResultSet(resultSet);
            }
        });

        return query;

    }

    public void clear(String table) {
        jdbcTemplate.update("DELETE FROM \"" + table + "\"");
    }

    public static void main(String[] args) {
        JdbcTools jdbcTools = JdbcTools.getInstance(1,"EEC_SHZ_01");

/*        Map<String, Object> map = new HashMap<>();
        map.put("ROW", kafkaConfig.getRowKey(3L, null));
        map.put("time", 3L);
        map.put("dp0", Bytes.toBytes(10.0));
        jdbcTools.upsert("test_p_d2_10W", map);
*/
//        List<Map<String, Object>> maps = jdbcTools.queryList("select * from \"test-1wpt-10p-d2_1W01\" WHERE \"time\" = 0");
//        List<Map<String, Object>> map2 = jdbcTools.queryList("select * from INDEX_DZB_P_D01");
/*
        String[] tableNames = new String[15];//{"irregular_data_201707","test-hbase2-height-2"};//,"test-1wpt-10p-d_10E01"};//"test-1wpt-10p-d_1E","test-1wpt-10p-d_10E",
        for (int i = 0; i < 6; i++) {
            int day = i + 7;
            String strday = day < 10 ? ("0" + day) : ("" + day);
            tableNames[i] = "irregular_data_2017" + strday;
        }
        for (int i = 0; i < 9; i++) {
            tableNames[i + 6] = "irregular_data_20180" + (i + 1);
        }
        for (String tablename : tableNames)
            jdbcTools.update("drop table \"" + tablename + "\"");
//        jdbcTools.update("drop table \"test-hbase2-height-2\"");
        System.out.println(234);
*/

        jdbcTools.clear("irregular_data_201809");

//        Map<String, Object> map = new LinkedHashMap<>();
//
//        map.put("ROW", "1000");
//        map.put("name", "小黑红");
//        map.put("age", "17");
//        long l = System.currentTimeMillis();
//        for (int i = 0; i < 100; i++) {
//            Object row = map.get("ROW");
//            map.put("ROW", (Integer.valueOf(row.toString()) + 1) + "");
//            jdbcTools.upsert("HBASE_FOR_P", map);
//        }
//        long l2 = System.currentTimeMillis();
//        System.out.println("时间：" + (l2 - l));
//
//        List<Map<String, Object>> maps = jdbcTools.queryList("select * from \"HBASE_FOR_P\"");
//
//        System.out.println(maps);
    }

}
