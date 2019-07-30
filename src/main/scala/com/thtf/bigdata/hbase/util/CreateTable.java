package com.thtf.bigdata.hbase.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.thtf.bigdata.util.JsonUtils;
import com.thtf.bigdata.util.PropertiesUtils;

/**
 * User: ieayoio
 * Date: 18-6-29
 */
public class CreateTable {
    private final static String FAMILY = PropertiesUtils.getPropertiesByKey("columnfamily.hbase");
    private final static String[] cols = {"time"}; // 除了循环重复额外的列
    private final static int dataBaseType = 1; // phoenix标识
    private static Logger log = Logger.getLogger(CreateTable.class);

    /**
     * 动态拼接建高表的sql语句: 用于测试hbase写入速度
     *
     * @param tableName: 表名
     * @return: sql
     */
    public static String createHTable(String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS \"").append(tableName).append(("\" (")).append("\n");
        sb.append("\"ROW\" VARBINARY NOT NULL PRIMARY KEY,").append("\n");
        sb.append("\"" + FAMILY + "\".").append("\"point\"").append(" UNSIGNED_INT").append(",").append("\n");
        for (String col : cols) {
            sb.append("\"" + FAMILY + "\".\"").append(col).append("\" UNSIGNED_INT").append(",").append("\n");
        }
        sb.append("\"" + FAMILY + "\".").append("\"value\"").append(" VARBINARY").append("\n");
        sb.append(")");
        return sb.toString();
    }

    /**
     * 动态拼接建高表的sql语句: 用于迁移mysql 流水表
     *
     * @param tableName: 表名
     * @return: sql
     */
    public static String createTableSQL4Mysql(String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS \"").append(tableName).append(("\" (")).append("\n");
        sb.append("\"ROW\" VARBINARY NOT NULL PRIMARY KEY,").append("\n");
        sb.append("\"" + FAMILY + "\".").append("\"point_id\"").append(" UNSIGNED_INT").append(",").append("\n");
        sb.append("\"" + FAMILY + "\".").append("\"sdate\"").append(" UNSIGNED_INT").append(",").append("\n");
        for (String col : cols) {
            sb.append("\"" + FAMILY + "\".\"").append(col).append("\" UNSIGNED_INT").append(",").append("\n");
        }
        sb.append("\"" + FAMILY + "\".").append("\"data\"").append(" VARBINARY").append("\n");
        sb.append(")");
        return sb.toString();
    }

    /**
     * 拼接建宽表的sql语句: 用于迁移mysql 参数表
     *
     * @return: sql
     */
    private static String[] createTableSQL4MySqlParamTable() {
        String[] sql_table = new String[5];
        sql_table[0] = "CREATE TABLE IF NOT EXISTS \"bay_tab\" (\n" +
                "\"" + FAMILY + "\".\"Description\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Of_Volt_Level\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Of_Substation\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Bay_No\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Bay_Graph\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Type\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Bay_State\" UNSIGNED_INT,\n" +
                "\"Name\" VARCHAR NOT NULL PRIMARY KEY\n" +
                ")";
        sql_table[1] = "CREATE TABLE IF NOT EXISTS \"ht_grp_tab\" (\n" +
                "\"" + FAMILY + "\".\"description\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"owner_code\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"grp_index\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"of_substation\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"grp_level\" UNSIGNED_INT,\n" +
                "\"grp_code\" VARCHAR NOT NULL PRIMARY KEY\n" +
                ")";
        sql_table[2] = "CREATE TABLE IF NOT EXISTS \"irregular_point_tab\" (\n" +
                "\"" + FAMILY + "\".\"point_code\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"diff_save\" VARBINARY,\n" +
                "\"" + FAMILY + "\".\"regular_save_time\" UNSIGNED_INT,\n" +
                "\"point_id\" UNSIGNED_INT NOT NULL PRIMARY KEY\n" +
                ")";
        sql_table[3] = "CREATE TABLE IF NOT EXISTS \"substation_tab\" (\n" +
                "\"" + FAMILY + "\".\"Description\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Type\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Of_SCA\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Of_Supplier\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Base_Volt\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Substation_No\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"GFS_Dband\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"GFS_Restore_Dband\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Security_Start_Year\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Security_Start_Month\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Security_Start_Day\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Dead_Data_Ratio\" VARBINARY,\n" +
                "\"" + FAMILY + "\".\"PAS_Calc_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Sub_Ctrl_No\" UNSIGNED_INT,\n" +
                "\"Name\" VARCHAR NOT NULL PRIMARY KEY\n" +
                ")";
        sql_table[4] = "CREATE TABLE IF NOT EXISTS \"ts_param_tab\" (\n" +
                "\"" + FAMILY + "\".\"Description\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Of_Substation\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Of_Volt_Level\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Of_Bay\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Type\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"RTU_No\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"TS_No\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Proto_Type_Id\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Info_Obj_Addr\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Dual_TS_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"TS_No2\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Calc_Var_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Desc_Locked\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Of_Area\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Acquis_Mode\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Print_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Logon_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Tone_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Graph_Pop_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Uncreat_Event\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"PDR_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"SOE_Express\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Event_Express\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Alarm_Graph\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Multi_Src_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Abnormal_State\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Fault_Limit_Value\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"State_Change_Times_Limit\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Fault_Judge_Mode\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Fault_Judge_Dband\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"PDR_Group_No\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Fault_Procedure_Name\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Reverse_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Transf_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Confirm_Mode\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Start_State\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Statistic_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Fault_Criterion\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Alarm_Process_Mode\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Alarm_Level\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"State_Change_Times_Limit_D\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Dev_Runtime_Limit\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"PUP_Act_Times_Limit\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Event_Delay\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Alarm_Delay\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"I_Threshold_Value\" VARBINARY,\n" +
                "\"" + FAMILY + "\".\"Onload_Amp\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Prot_Dev_Name\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Alarm_Code\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Of_Device\" VARCHAR,\n" +
                "\"" + FAMILY + "\".\"Snowfilt_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Display_Flag\" UNSIGNED_INT,\n" +
                "\"" + FAMILY + "\".\"Action_Name\" VARCHAR,\n" +
                "\"Name\" VARCHAR NOT NULL PRIMARY KEY\n" +
                ")";
        return sql_table;
    }

    /**
     * 拼接建索引的sql语句: 用于迁移mysql 参数表
     *
     * @param tables: 表名列表
     * @return: sql
     */

    private static String[] createIndexSQL4MySqlParamTable(String... tables) {
        String[] sql_index = new String[tables.length];
        int i = 0;
        for (String tab : tables) {
            String[] split = tab.split("\\.");
            sql_index[i] = getCreateIndexSql(split[0], FAMILY, split[1]);
            i++;
        }
        return sql_index;
    }

    /**
     * (宽表)生成Phoenix关联的sql语句
     *
     * @param tableName 创建的表名称
     * @param colNum    列的数目
     * @return
     */
    public static String createWTable(String tableName, int colNum) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS \"").append(tableName).append(("\" (")).append("\n");
        sb.append("\"ROW\" VARBINARY NOT NULL PRIMARY KEY,").append("\n");
        for (String col : cols) {
            sb.append("\"" + FAMILY + "\".\"").append(col).append("\" UNSIGNED_LONG").append(",").append("\n");
        }
        for (int i = 0; i < colNum; i++) {
            sb.append("\"" + FAMILY + "\".\"dp").append(i).append("\" VARBINARY");
            if (i != colNum - 1) {
                sb.append(",").append("\n");
            } else {
                sb.append("\n");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * 生成索引sql
     * 每个表对应一个索引,索引列不能为主键
     * @param tableName 表名
     * @param family    列族名
     * @param indexCols 需要建立的索引字段
     * @return 生成索引sql
     */
    public static String getCreateIndexSql(String tableName, String family, String... indexCols) {

        StringJoiner joiner = new StringJoiner(",");
        for (String c : indexCols) {
            joiner.add("\"" + family + "\".\"" + c + "\"");
        }
        String upTableName = tableName.toUpperCase().replaceAll("-|[.]", "_");
        String indexName = "INDEX_" + upTableName;
        return getIndexSql(indexName,tableName,joiner).toString();
    }

    /**
     * 生成索引sql
     * 适用于同表多索引
     * @param tableName 表名
     * @param index    索引序号： 多索引时以序号区分
     * @param indexCols 需要建立的索引字段
     * @return 生成索引sql
     */
    public static String getCreateIndexSql(String tableName, int index, JSONArray indexCols, JSONArray key) {
        StringJoiner joiner = new StringJoiner(",");
        for (int j = 0; j < indexCols.size(); j++) {
            String indexcol = indexCols.getString(j);
            boolean iskey = false;
            if (key != null && key.size() > 0) {
                for (int m = 0; m < key.size(); m++) {
                    String keystr = key.getString(m);
                    if (indexcol.equals(keystr)) {
                        iskey = true;
                        break;
                    }
                }
            }
            indexcol = iskey ? "\"" + indexcol + "\"" : "\"" + FAMILY + "\".\"" + indexcol + "\"";
            joiner.add(indexcol);
        }
        String upTableName = tableName.toUpperCase().replaceAll("-|[.]", "_");
        String indexName = "INDEX_" + upTableName + "_" + String.valueOf(index);
        return getIndexSql(indexName,tableName,joiner).toString();
    }

    private static StringBuilder getIndexSql(String indexName, String tableName, StringJoiner cols){
        StringBuilder stringBuilder = new StringBuilder("create local index IF NOT EXISTS ");
        stringBuilder
                .append(indexName)
                .append(" on ")
                .append("\"" + tableName + "\"")
                .append("(").append(cols.toString()).append(")");
        return stringBuilder;
    }
    /**
     * 调用HbaseApi建表
     *
     * @param tableName: 表名
     * @throws IOException
     */
    public static void createHbaseTable(String tableName, int type) throws IOException {
        String family = "c";
/*		byte[] startrow;
		byte[] endrow;
		if (isHeightTable) {
			startrow = kafkaConfig.getStartRow();
			endrow = kafkaConfig.getEndRow();
		} else {
			startrow = kafkaConfig.getStartRow_ForTime();
			endrow = kafkaConfig.getEndRow_ForTime();
		}
*/
        HBaseHelper.createTable(tableName, family, Integer.parseInt(PropertiesUtils.getPropertiesByKey("partitionCount.hbase")), type);
    }

    /**
     * 建hbase高表,并关联到phoenix
     *
     * @param tableNames:表名 列表
     * @throws IOException
     */
    public static void createHighTable(String... tableNames) throws IOException {
        JdbcTools jdbcTools = JdbcTools.getInstance(dataBaseType);
        for (String tableName : tableNames) {
            System.out.println("创建hbase表");
            createHbaseTable(tableName, 0);

            System.out.println("创建Phoenix表");
            String hTable = createHTable(tableName);
            jdbcTools.update(hTable);

            System.out.println("创建Phoenix索引");
            String index = getCreateIndexSql(tableName, "c", "point", "time");
            jdbcTools.update(index);
            System.out.println("创建Phoenix索引success");
        }
    }

    /**
     * 建hbase宽表,并关联到phoenix
     *
     * @param colCount:     列数
     * @param tableNames:表名 列表
     * @throws IOException
     */
    public static void createWeightTable(int colCount, String... tableNames) throws IOException {
        JdbcTools jdbcTools = JdbcTools.getInstance(dataBaseType);
        for (String tableName : tableNames) {
            System.out.println("创建hbase表");
            createHbaseTable(tableName, 0);

            System.out.println("创建Phoenix表");
            String hTable = createWTable(tableName, colCount);
            jdbcTools.update(hTable);

            System.out.println("创建Phoenix索引");
            String index = getCreateIndexSql(tableName, "c", "time");
            jdbcTools.update(index);
            System.out.println("创建Phoenix索引success");
        }
    }

    /**
     * mysql流水表
     * 建hbase高表,并关联到phoenix
     *
     * @param tableNames:表名 列表
     * @throws IOException
     */
    public static void createMySqlTable(String... tableNames) throws IOException {
        JdbcTools jdbcTools = JdbcTools.getInstance(dataBaseType);
        for (String tableName : tableNames) {
            System.out.println("创建hbase表");
            createHbaseTable(tableName, 1);

            System.out.println("创建Phoenix表");
            String hTable = createTableSQL4Mysql(tableName);
            jdbcTools.update(hTable);

            System.out.println("创建Phoenix索引");
            String index = getCreateIndexSql(tableName, "c", "point_id", "sdate", "time");
            jdbcTools.update(index);
            System.out.println("创建Phoenix索引success");
        }
    }

    /**
     * mysql参数表
     * 建hbase高表,并关联到phoenix
     *
     * @throws IOException
     */
    public static void createMySqlParamTable() throws IOException {
        JdbcTools jdbcTools = JdbcTools.getInstance(dataBaseType);
        String[] tableNames = {"bay_tab", "ht_grp_tab", "irregular_point_tab", "substation_tab", "ts_param_tab"};
        System.out.println("创建hbase表");
        for (String tableName : tableNames)
            createHbaseTable(tableName, 1);

        String[] phoenixTSql = createTableSQL4MySqlParamTable();
        System.out.println("创建Phoenix表");
        for (String sql : phoenixTSql)
            jdbcTools.update(sql);
        System.out.println("创建Phoenix表success");

        System.out.println("创建Phoenix索引");
        String[] tables = {"bay_tab.Of_Volt_Level", "irregular_point_tab.point_code", "ts_param_tab.Of_Bay"};
        String[] phoenixISql = createIndexSQL4MySqlParamTable(tables);
        for (String sql : phoenixISql)
            jdbcTools.update(sql);
        System.out.println("创建Phoenix索引success");
    }

    public static void main1(String[] args) {

        JdbcTools jdbcTools = JdbcTools.getInstance(dataBaseType);
        // 表名称
        String tableName = "test-1wpt-10p-d2_10W01";

        // 列数
        int colNum = 100000;

        // 生成创建表的sql
        String str = createHTable(tableName);
        System.out.println(str);

//        String index = getCreateIndexSql("dzb_p_d01", "c", "time", "point");
//        System.out.println(index);
//        jdbcTools.update(index);

        // 最好先运行这个方法让他输出一下看看sql是否正确，然后再解除注释下面两行运行，将会执行创建关联表
//
//        jdbcTools.update(str);

    }

    /**
     * 创建sqlserver对应表
     * 来源于kafka消费者
     *
     * @param o: 列信息json
     * @return: 0:创建成功,1: 解析JSON失败,2:创建失败,重试5次失败
     */
    public static int createTable(JSONObject o, int errorCount) {
        try {
            if (errorCount > 5) {
                return 2;
            }
            String tableName = o.getString("tablename");
            String nameSpace = o.getString("namespace");
            JSONObject columns = o.getJSONObject("columns");
            JSONArray indexs = o.getJSONArray("indexs");
            nameSpace = nameSpace.toUpperCase();
            int regioncount = o.getInteger("regioncount");
            regioncount = regioncount >= 6 ? regioncount : 6;
            try {
                HBaseHelper.createTableByNameSpace(nameSpace, tableName, FAMILY, regioncount);
                createSqlServerTable(nameSpace, tableName, columns, indexs);
            } catch (Exception e) {
                e.printStackTrace();
                errorCount++;
                deletePhoenixTable(nameSpace, tableName);
                deleteHbaseTable(nameSpace, tableName);
                return createTable(o, errorCount);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        return 0;
    }

    /**
     * 从Phoenix中删除数据表
     *
     * @param namespace 命名空间
     * @param tableName 表名称
     */
    public static void deletePhoenixTable(String namespace, String tableName) {
        try {
            JdbcTools jdbcTools = JdbcTools.getInstance(dataBaseType, namespace);
            jdbcTools.update("drop table if EXISTS \"" + tableName + "\"");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 从Hbase中删除数据表
     *
     * @param namespace 命名空间
     * @param tableName 表名称
     */
    public static void deleteHbaseTable(String namespace, String tableName) {
        try {
            HBaseHelper.deleteTable(namespace + ":" + tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createSqlServerTable(String namespace, String tableName, JSONObject columns, JSONArray indexs) throws Exception {
        JdbcTools jtschema = JdbcTools.getInstance(dataBaseType, "");
        System.out.println("创建Phoenix表");
        jtschema.update("CREATE SCHEMA IF NOT EXISTS " + namespace);

        JdbcTools jdbcTools = JdbcTools.getInstance(dataBaseType, namespace);
        jdbcTools.update(getCreateTableSql_phoenix(tableName, columns));
        System.out.println("创建Phoenix表成功");
        System.out.println("创建Phoenix索引");
        String[] s = new String[indexs.size()];
        String index = getCreateIndexSql(tableName, FAMILY, indexs.toArray(s));
        jdbcTools.update(index);
        System.out.println("创建Phoenix索引success");
    }

    public static String getCreateTableSql_phoenix(String tableName, JSONObject columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS \"").append(tableName).append(("\" (")).append("\n");
        sb.append("\"ROW\" VARBINARY NOT NULL PRIMARY KEY");
        LinkedHashMap<String, String> map = HBaseHelper.sqlServerDataTypeToPhoenix(columns);
        for (Map.Entry entry : map.entrySet()) {
            sb.append(",").append("\n").append("\"" + FAMILY + "\".\"").append(entry.getKey().toString()).append("\" ").
                    append(entry.getValue().toString());
        }
        sb.append(") COLUMN_ENCODED_BYTES = 0");
        return sb.toString();
    }

    /**
     * 获取phoenix建表语句
     * 支持复合主键
     *
     * @param tableName:表名
     * @param columns:列信息
     * @param isIMMUTABLE_ROWS:是否可变
     * @param regionCount:分区
     * @param primayKey:主键信息
     * @return
     */
    public static String getCreateTableSql_phoenix(String tableName, JSONObject columns, boolean isIMMUTABLE_ROWS, int regionCount, JSONArray primayKey) {
        if (primayKey == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS \"").append(tableName).append(("\" ("));
        LinkedHashMap<String, String> map = HBaseHelper.sqlServerDataTypeToPhoenix(columns);
        int i = 0;
        for (Map.Entry entry : map.entrySet()) {
            //主键
            boolean isPrimayKey = false;
            for (Object s : primayKey) {
                if (s.toString().equals(entry.getKey().toString())) {
                    isPrimayKey = true;
                    break;
                }
            }
            if (i > 0) {
                sb.append(",");
            }
            sb.append("\n\"");
            String col = entry.getKey().toString() + "\" " + entry.getValue().toString();
            if (isPrimayKey) {
                sb.append(col).append(" NOT NULL");//主键
            } else {
                sb.append(FAMILY).append("\".\"").append(col);//普通列
            }
            i++;
        }
        sb.append(",").append("\n");
        //主键
        if (primayKey.size() >= 0) {
            String priKey = "\"" + String.join("\",\"", primayKey.toArray(new String[primayKey.size()])) + "\"";
            sb.append("constraint UNION_PK primary key(").append(priKey).append(")");
        } else {
            sb.append("\"ROW\" BIGINT NOT NULL PRIMARY KEY");
        }
        //其它属性:压缩,编码,分区,是否可变表等
        sb.append(")").append("\n").append("COMPRESSION='SNAPPY',COLUMN_ENCODED_BYTES = 0");
        if (isIMMUTABLE_ROWS) {
            sb.append(",IMMUTABLE_ROWS=true,IMMUTABLE_STORAGE_SCHEME = ONE_CELL_PER_COLUMN");
        }
        if (regionCount > 0) {
            sb.append(",SALT_BUCKETS=").append(String.valueOf(regionCount));
        }
        return sb.toString();
    }

    /**
     * 以Phoenix方式创建表
     * 来源于kafka消费者
     *
     * @param o: 列信息json
     * @return: 0:创建成功,1: 解析JSON失败,2:创建失败,重试5次失败
     */
    public static int createPhoenixTable(JSONObject o, int errorCount) {
        try {
            if (errorCount > 5) {
                return 2;
            }
            //解析表结构信息
            String tableName = o.getString("tablename");
            String nameSpace = o.getString("namespace");
            JSONObject columns = o.getJSONObject("columns");
            JSONArray indexs = o.getJSONArray("indexs");
            JSONArray key = o.getJSONArray("rowkey");
            nameSpace = nameSpace.toUpperCase();
            int regioncount = o.getInteger("regioncount");
            regioncount = regioncount >= 6 ? regioncount : 6;
            try {
                //建命名空间
                createPhoenixNameSpace(nameSpace);
                //建表
                createPhoenixTable(nameSpace, tableName, columns, false, regioncount, key);
                //建索引
                createPhoenixIndex(nameSpace, tableName, indexs, key);
            } catch (Exception e) {
                e.printStackTrace();
                errorCount++;
                //迭代: 重新建表,5次
                return createPhoenixTable(o, errorCount);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        return 0;
    }

    /**
     * 创建公共表
     *
     * @param nameSpace:命名空间
     * @param tableName:表名称
     * @param columns:列信息
     * @param primayKey:主键
     */
    public static void createPhoenixTable(String nameSpace, String tableName, JSONObject columns, boolean isIMMUTABLE_ROWS, int regionCount, JSONArray primayKey) {
        JdbcTools jdbcTools = JdbcTools.getInstance(dataBaseType, nameSpace);
        if (primayKey == null || primayKey.size() <= 0) {
/*
            String sql = "CREATE SEQUENCE IF NOT EXISTS \"" + nameSpace + "\".\"SEQU_" + tableName + "\" START 1 CACHE 1000;";
            jdbcTools.update(sql);
*/
            log.error("待迁移表无主键，无法创建Hbase对应表");
            return;
        }
        jdbcTools.update(getCreateTableSql_phoenix(tableName, columns, isIMMUTABLE_ROWS, regionCount, primayKey));
        //迁移数据表无主键,创建自增sequence, 暂时不需要
        System.out.println("创建Phoenix表成功");
    }

    /**
     * 建索引
     *
     * @param nameSpace:命名空间
     * @param tableName:表名
     * @param indexs:索引列
     */
    public static void createPhoenixIndex(String nameSpace, String tableName, JSONArray indexs, JSONArray key) {
        if (indexs == null || indexs.size() <= 0) {
            log.info("待迁移表没有索引，无需创建");
            return;
        }
        JdbcTools jdbcTools = JdbcTools.getInstance(dataBaseType, nameSpace);
        System.out.println("创建Phoenix索引");
        for (int i = 0; i < indexs.size(); i++) {
            JSONArray index = indexs.getJSONArray(i);
            if (index == null || index.size() <= 0) return;
            String indexSql = getCreateIndexSql(tableName, i, index,key);
            jdbcTools.update(indexSql);
            System.out.println("创建Phoenix索引\""+index.toJSONString()+"\"success");
        }
    }

    /**
     * 建命名空间
     *
     * @param nameSpace:命名空间名称
     */
    public static void createPhoenixNameSpace(String nameSpace) {
        JdbcTools jtschema = JdbcTools.getInstance(dataBaseType, "");
        System.out.println("创建Phoenix表空间");
        jtschema.update("CREATE SCHEMA IF NOT EXISTS " + nameSpace);
        System.out.println("创建Phoenix表空间成功");
    }

    public static void main(String[] args) throws IOException {
//        String[] wtableNames = {"test-hbase2-weight-1"};//,"test-hbase2-weight-2"};
        int colCount = 10000;
//    	createWeightTable(colCount, wtableNames);
/*    	String[] tableNames = new String[15];//{"irregular_data_201707","test-hbase2-height-2"};//,"test-1wpt-10p-d_10E01"};//"test-1wpt-10p-d_1E","test-1wpt-10p-d_10E",
    	for (int i = 0; i< 6; i++) {
    		int day = i+7;
    		String strday = day < 10 ? ("0" + day): (""+day);
    		tableNames[i] = "irregular_data_2017" + strday;
    	}
    	for (int i = 0; i< 9; i++) {
    		tableNames[i+6] = "irregular_data_20180" + (i+1);
    	}
*/
//String[] tableNames = {"irregular_data_201802","irregular_data_201806"};//,"test-1wpt-10p-d_10E01"};//"test-1wpt-10p-d_1E","test-1wpt-10p-d_10E",
//    	createHighTable(tableNames);
        //      createMySqlParamTable();
//        CreateTable ct = new CreateTable();
        //      ct.createTable(null, 0);
//        deleteTable("EEC_SHZ_01","irregular_data_201809");
        String[] array = {"12", "af", "fff"};
        String a = array.toString();
        List l = Arrays.asList(array);
        String b = l.toString();
        System.out.println(a);
        System.out.println(b);
        System.out.println(String.join(",", array));
        System.out.println(String.join(",",new String[]{"test0"}));
//        deletePhoenixTable("EEC_SHZ_01", "irregular_data_201810");
  //      deleteHbaseTable("EEC_SHZ_01", "irregular_data_201810");
        String json = "{\"namespace\":\"EEC_SHZ_01\",\n" +
                "    \"tablename\": \"irregular_data_201810\",\n" +
                "    \"columns\": {\n" +
                "        \"point_id\": \"int\",\n" +
                "        \"sdate\": \"int\",\n" +
                "        \"time\": \"int\",\n" +
                "        \"data\": \"float\"\n" +
                "    },\n" +
                "    \"indexs\": [],\n" +
                "\t\"regioncount\":12,\n" +
                "\t\"rowkey\":[\"point_id\",\"sdate\",\"time\"]\t//主键唯一\n" +
                "\t}";
        JSONObject o = JsonUtils.StringToJSONObject(json);
        // createPhoenixTable(o, 0);"point_id","data"
        String indexstr = "[[\"sdate\"],[\"point_id\",\"sdate\"],[\"data\"],[\"point_id\",\"data\"]]";
        String keystr = "[\"point_id\",\"sdate\",\"time\"]";

        createPhoenixIndex("STORM_TEST","TABLE1002",JSONArray.parseArray(indexstr),JSONArray.parseArray(keystr));
    }

}
