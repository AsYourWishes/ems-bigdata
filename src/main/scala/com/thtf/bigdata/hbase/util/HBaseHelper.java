package com.thtf.bigdata.hbase.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTinyint;

import com.alibaba.fastjson.JSONObject;
import com.thtf.bigdata.util.PropertiesUtils;

/**
 * hbase相关操作
 *
 * @author gk-dingml
 */
public class HBaseHelper {
    public static final int writeBufferSize = 10;

    // 声明静态配置
    private static Configuration conf = null;
    private static Connection conn = null;
    private static String coprocessClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";

    /**
     * 获取全局唯一的Configuration实例
     *
     * @return
     */
    public static synchronized Configuration getConfiguration() {
        try {
            if (conf == null) {
                conf = HBaseConfiguration.create();
                // conf.set("hbase.zookeeper.property.clientPort",
                // ConfigUtil.getInstance().getConfigVal("zkport",
                // ConstantProperties.COMMON_PROP));
                conf.set("hbase.zookeeper.quorum", "dn0,dn1,dn2");
                conf.set("zookeeper.znode.parent", "/hbase-unsecure");
                conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 12000000);
            }
        } catch (Exception e) {
            conf = null;
            throw new RuntimeException(e);
        }
        return conf;
        // conf = HBaseConfiguration.create();
        // conf.set("hbase.zookeeper.quorum", "dn0.hadoop,dn1.hadoop,dn2.hadoop");
        // conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        // conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 120000);

    }

    /**
     * 获取全局唯一的HConnection实例
     *
     * @return
     * @throws IOException
     * @throws ZooKeeperConnectionException
     */
    public static synchronized Connection getHConnection() throws IOException {
        if (conn == null || conn.isClosed()) {
            conn = ConnectionFactory.createConnection(getConfiguration());
        }

        return conn;
    }

    public static void main(String[] args) {
        try {
            System.out.println(Long.MAX_VALUE);
            String f = "c";
            String tableName = "test-qt-002";
            short s = 0;
            short s1 = Short.MAX_VALUE;
            Long l = 0l;
            Long l1 = Long.MAX_VALUE;
            byte[] strowkeyhead = Bytes.toBytes(s);
            byte[] strowkeytail = Bytes.toBytes(l);
            byte[] strowKey = Bytes.add(strowkeyhead, strowkeytail);
            byte[] endrowkeyhead = Bytes.toBytes(s1);
            byte[] endrowkeytail = Bytes.toBytes(l1);
            byte[] endrowKey = Bytes.add(endrowkeyhead, endrowkeytail);
            // createTable(tableName, f, strowKey, endrowKey, 50);
            // getRows(null, null, null, null);
        } catch (Throwable e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    // 创建数据库表
    @SuppressWarnings("deprecation")
    public static void createTable(String tableName, String... columnFamilys) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 创建一个数据库管理员
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
        if (hAdmin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + "表已存在");
            // conn.close();
            // System.exit(0);
        } else {
            // 新建一个表描述
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            // 在表描述里添加列族
            for (String columnFamily : columnFamilys) {
                tdb.setColumnFamily(getTableDescriptor(columnFamily).build());
            }
            // 根据配置好的表描述建表
            hAdmin.createTable(tdb.build());
            System.out.println("创建" + tableName + "表成功");
        }
        hAdmin.close();
    }

    /**
     * 创建数据库表
     *
     * @param tableName:表名
     * @param columnFamily:列族名
     * @param regionnum:分区数
     * @param type:            分区类型: 0: 测试数据,1: 早期mysql迁移
     * @throws IOException
     */
    public static void createTable(String tableName, String columnFamily, int regionnum,
                                   int type) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 创建一个数据库管理员
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
        if (hAdmin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + "表已存在");
            // conn.close();
            // System.exit(0);
        } else {

            // 新建一个表描述
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            // tableDesc.addCoprocessor(coprocessClassName);
            tdb.setColumnFamily(getTableDescriptor(columnFamily).build());
//			tdb.setCoprocessome(coprocessClassName);
//			hAdmin.createTable(tdb.build());
            // 根据配置好的表描述建表
//            if (startkey == null) {
            byte[][] regions = new byte[regionnum - 1][];
            int base = type == 1 ? 1 : Math.round((float) 10000 / regionnum);
            for (int i = 0; i < regionnum - 1; i++) {
                regions[i] = Bytes.toBytes(Short.parseShort(String.valueOf((i + 1) * base)));
            }
            hAdmin.createTable(tdb.build(), regions);
/*
            } else {
                hAdmin.createTable(tdb.build(), startkey, endkey, regionnum);
            }
*/
            System.out.println("创建" + tableName + "表成功");
        }
        hAdmin.close();
    }

    // 创建数据库表
    public static void createTableByNameSpace(String nameSpace, String tableName, String columnFamily, int regionnum) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 创建一个数据库管理员
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
        nameSpace = nameSpace.toUpperCase();
        createNameSpace(hAdmin, nameSpace);
        TableName tn = TableName.valueOf(nameSpace + ":" + tableName);
        if (hAdmin.tableExists(tn)) {
            System.out.println(tn.getNameAsString() + "表已存在");
            // conn.close();
            // System.exit(0);
        } else {
            // 新建一个表描述
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
            // tableDesc.addCoprocessor(coprocessClassName);
            tdb.setColumnFamily(getTableDescriptor(columnFamily).build());
            // 根据配置好的表描述建表
            byte[][] regions = new byte[regionnum - 1][];
            int base = Math.round((float) 1000 / regionnum);
            for (int i = 0; i < regionnum - 1; i++) {
                regions[i] = Bytes.toBytes(Short.parseShort(String.valueOf((i + 1) * base)));
            }
            hAdmin.createTable(tdb.build(), regions);
            System.out.println("创建" + tableName + "表成功");
        }
        hAdmin.close();
    }

    /**
     * 创建命名空间： 先判断是否存在
     *
     * @param hba:           hbaseAdmin数据引擎
     * @param nameSpace：命名空间
     * @throws IOException
     */
    private static void createNameSpace(HBaseAdmin hba, String nameSpace) throws IOException {
        NamespaceDescriptor[] nameSpaceList = hba.listNamespaceDescriptors();
        boolean hasename = false;
        for (NamespaceDescriptor ns : nameSpaceList) {
            if (nameSpace.equals(ns.getName())) {
                hasename = true;
                break;
            }
        }
        if (!hasename) {
            hba.createNamespace(NamespaceDescriptor.create(nameSpace).build());
        }
    }

    /**
     * 创建列属性bulider
     *
     * @param columnFamily:列族名称
     * @return ColumnFamilyDescriptorBuilder
     */
    private static ColumnFamilyDescriptorBuilder getTableDescriptor(String columnFamily) {
        // 在表描述里添加列族
        ColumnFamilyDescriptorBuilder colDesc = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(columnFamily));
        // bloom过滤器，过滤加速
        colDesc.setBloomFilterType(BloomType.ROW);
        // 压缩内存和存储中的数据，内存紧张的时候设置
        colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX);
        // 让数据块缓存在LRU缓存里面有更高的优先级
        colDesc.setInMemory(true);
        // 最大版本，没必要的话，就设置成1个
        colDesc.setMaxVersions(1);
        // 集群间复制的时候，如果被设置成REPLICATION_SCOPE_LOCAL就不能被复制了
        colDesc.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
        // 存储的时候使用压缩算法，这个基本是必备的，hbase的存储大得惊人
        colDesc.setCompressionType(Algorithm.SNAPPY);
        // 进行compaction的时候使用压缩算法
        colDesc.setCompactionCompressionType(Algorithm.SNAPPY);
        return colDesc;
    }

    public static void modifyTable(String tableName, String familyName) {
        try {
            Connection conn = getHConnection();
            TableName tname = TableName.valueOf(tableName);
            HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
            if (hAdmin.tableExists(tname)) {
                // 判断是否可以获取
                if (hAdmin.isTableAvailable(tname)) {

                    if (!hAdmin.isTableDisabled(tname)) {
                        // 如果没关闭则关闭
                        hAdmin.disableTable(tname);
                        System.out.println(tableName + " disable...");
                    }
                    TableDescriptorBuilder tableDesc = TableDescriptorBuilder.newBuilder(hAdmin.getDescriptor(tname));
                    try {
//						tableDesc.setCoprocessor(coprocessClassName);
                        hAdmin.modifyTable(tname, tableDesc.build());
                        /*
                         * HColumnDescriptor colDesc = tableDesc .getFamily(Bytes.toBytes(familyName));
                         * if(colDesc == null){ System.out.println(familyName + " is null column ");
                         * }else{ // modifying existing ColumnFamily // bloom过滤器，过滤加速
                         * colDesc.setBloomFilterType(BloomType.ROW); // 压缩内存和存储中的数据，内存紧张的时候设置
                         * colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX); //
                         * 让数据块缓存在LRU缓存里面有更高的优先级 colDesc.setInMemory(true); // 最大版本，没必要的话，就设置成1个
                         * colDesc.setMaxVersions(1); // 集群间复制的时候，如果被设置成REPLICATION_SCOPE_LOCAL就不能被复制了
                         * colDesc.setScope(HConstants.REPLICATION_SCOPE_GLOBAL); //
                         * 存储的时候使用压缩算法，这个基本是必备的，hbase的存储大得惊人
                         * colDesc.setCompressionType(Algorithm.SNAPPY); // 进行compaction的时候使用压缩算法
                         * colDesc.setCompactionCompressionType(Algorithm.SNAPPY);
                         * hAdmin.modifyColumn(tableName, colDesc);
                         *
                         * }
                         */
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if (!hAdmin.isTableEnabled(tname)) {
                        // 如果没有打开则打开表
                        hAdmin.enableTable(tname);
                        System.out.println(tableName + " enable...");
                    }

                    System.out.println(tableName + " add And modify column success!");
                } else {
                    System.out.println(tableName + " is not available!");
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    // 添加一条数据
    public static void addRow(String tableName, String rowKey, String columnFamily, String column, String value)
            throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 通过rowkey创建一个put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 在put对象中设置列族、列、值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        // 插入数据,可通过put(List<Put>)批量插入
//        table.put(put);
        table.checkAndMutate(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamily)).qualifier(Bytes.toBytes(column)).ifNotExists().thenPut(put);
        // 关闭资源
        table.close();

    }

    // 添加一条数据
    public static void addRow(String tableName, byte[] rowKey, String columnFamily, String column, String value)
            throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 通过rowkey创建一个put对象
        Put put = new Put(rowKey);
        // 在put对象中设置列族、列、值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        // 插入数据,可通过put(List<Put>)批量插入
        table.put(put);
        // 关闭资源
        table.close();

    }

    // 批量添加数据，一次只添加一行
    public static void addRows(String tableName, String[] rowKeys, String columnFamily, String[] columns,
                               String[] values) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<Put>();
        int i;
        for (i = 0; i < columns.length; ++i) {
            // 通过rowkey创建一个put对象
            Put put = new Put(Bytes.toBytes(rowKeys[i]));
            // 在put对象中设置列族、列、值
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
            // 插入数据,可通过put(List<Put>)批量插入
            puts.add(put);
        }
        table.put(puts);
        // 关闭资源
        table.close();

    }

    // 批量添加数据2，一次添加多行，每行只有一列
    public static void addRows(String tableName, String[][] rowKeys, String columnFamily, String[][] columns,
                               String[][] values) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<Put>();
        Put put;
        int i, j;
        for (i = 0; i < rowKeys.length; ++i) {

            for (j = 0; j < columns[i].length; ++j) {
                // 通过rowkey创建一个put对象
                put = new Put(Bytes.toBytes(rowKeys[i][j]));
                // 在put对象中设置列族、列、值
                // System.out.println(i+"\t"+j);
                try {
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i][j]),
                            Bytes.toBytes(values[i][j]));
                } catch (Exception e) {
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i][j]),
                            Bytes.toBytes(String.format("%d", -1000)));
                } finally {

                }
                // 插入数据,可通过put(List<Put>)批量插入
                puts.add(put);
            }
            if (0 == i % 10000) {
                System.out.println(String.format("%s, row: %d\n", tableName, i));
            }
        }
        table.put(puts);
        // 关闭资源
        table.close();

    }

    // 批量添加数据3，一次添加多行，每行有多列
    public static void addRows(String tableName, List<byte[]> rowKeys, String columnFamily, String[][] columns,
                               String[][] values) throws IOException {
        // 建立一个数据库的连接
        Connection conn = null;
        Long sdate = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        System.out.println("开始插入时间:" + LocalDateTime.now().toString() + "ms");
        try {
            conn = getHConnection();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        Long cdate = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        System.out.println("连接时间:" + (cdate - sdate) + "ms");

        // 获取表
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        Long tdate = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        System.out.println("连接时间:" + (tdate - cdate) + "ms");
        Put put;
        int i, j;
        List<Put> puts = new ArrayList<Put>();
        // table.setAutoFlushTo(false);
        for (i = 0; i < rowKeys.size(); ++i) {
            // 通过rowkey创建一个put对象
            if (null == rowKeys.get(i)) {
                continue;
            }
            put = new Put(rowKeys.get(i));
            for (j = 0; j < columns[i].length; ++j) {
                // 在put对象中设置列族、列、值
                // System.out.println(i+"\t"+j);
                try {
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i][j]),
                            Bytes.toBytes(values[i][j]));
                } catch (Exception e) {
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i][j]),
                            Bytes.toBytes(String.format("%d", -1000)));
                }
                // 插入数据,可通过put(List<Put>)批量插入
                puts.add(put);
            }
        }
        table.put(puts);
        Long idate = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        System.out.println("连接时间:" + (idate - tdate) + "ms");
        // 关闭资源
        try {
            table.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    // 通过rowkey获取一条数据
    public static Map getRow(String tableName, String rowKey) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 通过rowkey创建一个get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        // 输出结果
        Result result = table.get(get);
        Map resultMap = new HashMap<String, String>();
        for (Cell cell : result.rawCells()) {
            resultMap.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("行键:" + new String(CellUtil.cloneRow(cell)) + "\t" + "列族:"
                    + new String(CellUtil.cloneFamily(cell)) + "\t" + "列名:" + new String(CellUtil.cloneQualifier(cell))
                    + "\t" + "值:" + new String(CellUtil.cloneValue(cell)) + "\t" + "时间戳:" + cell.getTimestamp());
        }
        // 关闭资源
        table.close();
        return resultMap;
    }

    // 通过rowkey获取多条数据
    public static HashMap<String, String> getRows(String tableName, String startTime, String endTime,
                                                  ArrayList<String> alCondition) throws Throwable {
        HashMap<String, String> hm = new HashMap<String, String>();
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startTime));
        // scan.setStartRow(Bytes.toBytes(startTime));
        scan.withStopRow(Bytes.toBytes(endTime));

        RowRange rowRange1 = new RowRange(startTime, true, endTime, false);
        RowRange rowRange2 = new RowRange(startTime, false, endTime, true);
        List<RowRange> rowRanges = new ArrayList<RowRange>();
        rowRanges.add(rowRange1);
        rowRanges.add(rowRange2);
        Filter filterArange = new MultiRowRangeFilter(rowRanges);

        FilterList fl = new FilterList();
        Filter f;
        if (null != alCondition) {
            // 遍历查询条件
            // List<RowRange> ranges = new ArrayList<RowRange>();
            for (String condition : alCondition) {
                f = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(condition));
                fl.addFilter(f);

            }
        }
        fl.addFilter(filterArange);
        scan.setFilter(fl);

        // 输出结果
        ResultScanner rs = null;
        String rowkey = null;

        LocalDateTime dt = LocalDateTime.now();
        /*
         * System.out.println(dt.getYear() + "-" + dt.getMonthValue() + "-" +
         * dt.getDayOfMonth() + " " + dt.getHour() + ":" + dt.getMinute() + ":" +
         * dt.getSecond());
         */
        rs = table.getScanner(scan);
        // scan.setBatch(100000);
        // System.out.println(scan.getBatch());
        dt = LocalDateTime.now();
        /*
         * System.out.println(dt.getYear() + "-" + dt.getMonthValue() + "-" +
         * dt.getDayOfMonth() + " " + dt.getHour() + ":" + dt.getMinute() + ":" +
         * dt.getSecond());
         */
        String rowKey, values;
        for (Result r : rs) {
            rowKey = "";
            values = "";
            for (Cell cell : r.rawCells()) {
                /*
                 * System.out.println("行键:" + new String(CellUtil.cloneRow(cell)) + "\t" + "列族:"
                 * + new String(CellUtil.cloneFamily(cell)) + "\t" + "列名:" + new
                 * String(CellUtil.cloneQualifier(cell)) + "\t" + "值:" + new
                 * String(CellUtil.cloneValue(cell)) + "\t" + "时间戳:" + cell.getTimestamp());
                 * lRow++;
                 */
                rowKey = new String(CellUtil.cloneRow(cell));
                values = values + "," + new String(CellUtil.cloneValue(cell));
            }
            values = values.substring(1, values.length());
            hm.put(rowKey, values);
            /*
             * if( 0 == lRow%1000 ) { dt = LocalDateTime.now();
             * System.out.println(dt.getYear() + "-" + dt.getMonthValue() + "-" +
             * dt.getDayOfMonth() + " " + dt.getHour() + ":" + dt.getMinute() + ":" +
             * dt.getSecond()); System.out.println(lRow); }
             */
        }

        dt = LocalDateTime.now();
        /*
         * System.out.println(dt.getYear() + "-" + dt.getMonthValue() + "-" +
         * dt.getDayOfMonth() + " " + dt.getHour() + ":" + dt.getMinute() + ":" +
         * dt.getSecond());
         */
        // System.out.println(hm.size());
        // 关闭资源
        rs.close();
        table.close();

        return hm;
    }

    // 通过rowkey和列获取多条数据
    public static HashMap<String, String> getRows1(String tableName, String familyName, String ColName,
                                                   String startTime, String endTime, ArrayList<String> alCondition) throws Throwable {
        HashMap<String, String> hm = new HashMap<String, String>();
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        List<RowRange> rowRanges = new ArrayList<RowRange>();

        FilterList fl = new FilterList();
        Filter f;
        if (null != alCondition) {
            // 遍历查询条件
            List<RowRange> ranges = new ArrayList<RowRange>();
            for (String condition : alCondition) {
                f = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(condition));
                fl.addFilter(f);

            }
        }
        Filter course_art_filter1 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(ColName),
                CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes(startTime));
        Filter course_art_filter2 = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(ColName),
                CompareOperator.LESS_OR_EQUAL, Bytes.toBytes(endTime));
        fl.addFilter(course_art_filter1);
        fl.addFilter(course_art_filter2);
        scan.setFilter(fl);

        // 输出结果
        ResultScanner rs = null;
        String rowkey = null;

        LocalDateTime dt = LocalDateTime.now();
        /*
         * System.out.println(dt.getYear() + "-" + dt.getMonthValue() + "-" +
         * dt.getDayOfMonth() + " " + dt.getHour() + ":" + dt.getMinute() + ":" +
         * dt.getSecond());
         */
        rs = table.getScanner(scan);
        // scan.setBatch(100000);
        // System.out.println(scan.getBatch());
        dt = LocalDateTime.now();
        /*
         * System.out.println(dt.getYear() + "-" + dt.getMonthValue() + "-" +
         * dt.getDayOfMonth() + " " + dt.getHour() + ":" + dt.getMinute() + ":" +
         * dt.getSecond());
         */
        String rowKey, values;
        for (Result r : rs) {
            rowKey = "";
            values = "";
            for (Cell cell : r.rawCells()) {
                /*
                 * System.out.println("行键:" + new String(CellUtil.cloneRow(cell)) + "\t" + "列族:"
                 * + new String(CellUtil.cloneFamily(cell)) + "\t" + "列名:" + new
                 * String(CellUtil.cloneQualifier(cell)) + "\t" + "值:" + new
                 * String(CellUtil.cloneValue(cell)) + "\t" + "时间戳:" + cell.getTimestamp());
                 * lRow++;
                 */
                rowKey = new String(CellUtil.cloneRow(cell));
                values = values + "," + new String(CellUtil.cloneValue(cell));
            }
            values = values.substring(1, values.length());
            hm.put(rowKey, values);
            /*
             * if( 0 == lRow%1000 ) { dt = LocalDateTime.now();
             * System.out.println(dt.getYear() + "-" + dt.getMonthValue() + "-" +
             * dt.getDayOfMonth() + " " + dt.getHour() + ":" + dt.getMinute() + ":" +
             * dt.getSecond()); System.out.println(lRow); }
             */
        }

        dt = LocalDateTime.now();
        /*
         * System.out.println(dt.getYear() + "-" + dt.getMonthValue() + "-" +
         * dt.getDayOfMonth() + " " + dt.getHour() + ":" + dt.getMinute() + ":" +
         * dt.getSecond());
         */
        System.out.println(hm.size());
        // 关闭资源
        rs.close();
        table.close();

        return hm;
    }

    // 全表扫描
    public static void scanTable(String tableName, byte[] startkey, byte[] endkey) throws IOException {

        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 创建一个扫描对象
        Scan scan = new Scan();
        /*
         * FilterList fl = new FilterList(); Short s = 0; Short es = 999; Filter filter1
         * = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,new
         * ByteArrayComparable(Bytes.add(Bytes.toBytes(s),startkey)) {
         *
         * @Override public byte[] toByteArray() { // TODO Auto-generated method stub
         * return null; }
         *
         * @Override public int compareTo(byte[] abyte0, int i, int j) { // TODO
         * Auto-generated method stub return 0; } });
         *
         * Filter filter2 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,new
         * ByteArrayComparable(Bytes.add(Bytes.toBytes(es),endkey)) {
         *
         * @Override public byte[] toByteArray() { // TODO Auto-generated method stub
         * return null; }
         *
         * @Override public int compareTo(byte[] abyte0, int i, int j) { // TODO
         * Auto-generated method stub return 0; } }); fl.addFilter(filter1);
         * fl.addFilter(filter2); scan.setFilter(fl);
         */
        if (startkey != null) {
            scan.withStartRow(startkey);
        }
        if (endkey != null) {
            scan.withStopRow(endkey);
        }
        scan.setBatch(10);
        // 扫描全表输出结果
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                String famliy = new String(CellUtil.cloneFamily(cell));
                if (!"c".equals(famliy)) {
                    continue;
                }
                String colname = new String(CellUtil.cloneQualifier(cell));
                byte[] valb = CellUtil.cloneValue(cell);
                String val = "time".equals(colname) ? String.valueOf(Bytes.toLong(valb))
                        : "value".equals(colname) ? String.valueOf(Bytes.toDouble(valb)) : new String(valb);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] r = Bytes.copy(rowkey, 2, rowkey.length - 10);
                System.out.println("\u884c\u952e:" + // String.valueOf(Bytes.toString(rowkey)) + "\t"
                        String.valueOf(Bytes.toShort(Bytes.head(rowkey, 2))) + Bytes.toString(r)
                        + String.valueOf(Bytes.toLong(Bytes.tail(rowkey, 8))) + "\t" + "\u5217\u65cf:"
                        + new String(CellUtil.cloneFamily(cell)) + "\t" + "\u5217\u540d:" + colname + "\t" + "\u503c:"
                        + val + "\t"
                        // + new String(String.valueOf(Bytes.toDouble(CellUtil.cloneValue(cell)))) +
                        // "\t"
                        + "\u65f6\u95f4\u6233:" + cell.getTimestamp());
            }
        }
        // 关闭资源
        results.close();
        table.close();

    }

    // 删除一条数据
    public static void delRow(String tableName, String rowKey) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        // 关闭资源
        table.close();

    }

    // 删除多条数据
    public static void delRows(String tableName, String[] rows) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 删除多条数据
        List<Delete> list = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            list.add(delete);
        }
        table.delete(list);
        // 关闭资源
        table.close();

    }

    // 清空一个表
    public static void truncateTable(String tableName) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 创建一个数据库管理员
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
        // 清空指定表
        hAdmin.disableTable(TableName.valueOf(tableName));
        hAdmin.truncateTable(TableName.valueOf(tableName), false);
    }

    // 删除列族
    public static void delColumnFamily(String tableName, String columnFamily) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 创建一个数据库管理员
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
        // 删除一个表的指定列族
        hAdmin.deleteColumnFamily(TableName.valueOf(tableName), Bytes.toBytes(columnFamily));
        // 关闭资源

    }

    // 删除数据库表
    public static void deleteTable(String tableName) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 创建一个数据库管理员
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if (hAdmin.tableExists(tn)) {
            // 失效表
            hAdmin.disableTable(tn);
            // 删除表
            hAdmin.deleteTable(tn);
            System.out.println("删除" + tableName + "表成功");

        } else {
            System.out.println("需要删除的" + tableName + "表不存在");
        }
    }

    // 追加插入(将原有value的后面追加新的value，如原有value=a追加value=bc则最后的value=abc)
    public static void appendData(String tableName, String rowKey, String columnFamily, String column, String value)
            throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 通过rowkey创建一个append对象
        Append append = new Append(Bytes.toBytes(rowKey));
        // 在append对象中设置列族、列、值
        append.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        // 追加数据
        table.append(append);
        // 关闭资源
        table.close();

    }

    // 符合条件后添加数据(只能针对某一个rowkey进行原子操作)
    public static boolean checkAndPut(String tableName, String rowKey, String columnFamilyCheck, String columnCheck,
                                      String valueCheck, String columnFamily, String column, String value) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 设置需要添加的数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        // 当判断条件为真时添加数据
        boolean result = table.checkAndMutate(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamilyCheck))
                .qualifier(Bytes.toBytes(columnCheck)).ifEquals(Bytes.toBytes(valueCheck)).thenPut(put);

        // 关闭资源
        table.close();

        return result;
    }

    // 符合条件后刪除数据(只能针对某一个rowkey进行原子操作)
    public static boolean checkAndDelete(String tableName, String rowKey, String columnFamilyCheck, String columnCheck,
                                         String valueCheck, String columnFamily, String column) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 设置需要刪除的delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamilyCheck), Bytes.toBytes(columnCheck));
        // 当判断条件为真时添加数据
//		boolean result = table.checkAndDelete(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamilyCheck),
        //			Bytes.toBytes(columnCheck), Bytes.toBytes(valueCheck), delete);
        boolean result = table.checkAndMutate(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamilyCheck))
                .qualifier(Bytes.toBytes(columnCheck)).ifEquals(Bytes.toBytes(valueCheck)).thenDelete(delete);
        // 关闭资源
        table.close();

        return result;
    }

    // 计数器(amount为正数则计数器加，为负数则计数器减，为0则获取当前计数器的值)
    public static long incrementColumnValue(String tableName, String rowKey, String columnFamily, String column,
                                            long amount) throws IOException {
        // 建立一个数据库的连接
        Connection conn = getHConnection();
        // 获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 计数器
        long result = table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamily),
                Bytes.toBytes(column), amount);
        // 关闭资源
        table.close();

        return result;
    }

    public static long getRowCount(String tableName, String family, String col) {
/*				Scan s = new Scan();
        s.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
        AggregationClient ac = new AggregationClient(getConfiguration());
        try {
            return ac.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), s);
        } catch (Throwable e) {
           e.printStackTrace();
        }
*/
        return 0;
    }

    /**
     * 生成rowkey
     *
     * @param time      时间戳
     * @param pointname 点名称,宽表时传null
     * @return
     */
    public static byte[] getRowKey(Long time, String pointname) {
        //时间戳反转
        Long dTail = Long.MAX_VALUE - time;
        //高表直接取点的hash, 宽表取 ("dp"+ 时间戳反转) 的hash
        int hash = pointname == null ? ("dp" + String.valueOf(dTail)).hashCode() : pointname.hashCode();
        //取绝对值,去除负号
        String strHead1 = String.valueOf(Math.abs(hash));
        //截取后4位
        String strHead = strHead1.length() < 4 ? strHead1 : strHead1.substring(strHead1.length() - 4);
        int strlen = strHead.length();
        StringBuilder sb = new StringBuilder();
        //反转后四位
        while (strlen > 0) {
            sb.append(strHead.charAt(strlen - 1));
            strlen--;
        }
        short sHead = Short.parseShort(sb.toString());
        // int sHead = dTail.hashCode();
        // System.out.println(sHead);
        byte[] rowkeyhead = Bytes.toBytes(sHead);
        byte[] rowkeytail = Bytes.toBytes(dTail);
        byte[] rowKey = null;
        if (pointname == null) {
            rowKey = Bytes.add(rowkeyhead, rowkeytail);
        } else {
            byte[] pointbyte = Bytes.toBytes(pointname);
            rowKey = Bytes.add(rowkeyhead, pointbyte, rowkeytail);
        }
        return rowKey;
    }

    public static byte[] getStartRow() {
        short shead = 0;
        String pointid = "dp0";
        long stail = 0;
        return Bytes.add(Bytes.toBytes(shead), Bytes.toBytes(pointid), Bytes.toBytes(stail));
    }

    public static byte[] getEndRow() {
        short shead = 9999;
        String pointid = "dp9999";
        long stail = Long.MAX_VALUE;
        return Bytes.add(Bytes.toBytes(shead), Bytes.toBytes(pointid), Bytes.toBytes(stail));
    }

    public static byte[] getStartRow_ForTime() {
        short shead = 0;
        long stail = 0;
        return Bytes.add(Bytes.toBytes(shead), Bytes.toBytes(stail));
    }

    public static byte[] getEndRow_ForTime() {
        short shead = 9999;
        long stail = Long.MAX_VALUE;
        return Bytes.add(Bytes.toBytes(shead), Bytes.toBytes(stail));
    }

    public static byte[] getRowkey_mysql(int pointid, int days, int time) {
        int hashcode = (pointid ^ days) % Integer.parseInt(PropertiesUtils.getPropertiesByKey("partitionCount.hbase"));
        String keystr = String.valueOf(hashcode) + String.valueOf(pointid) +
                String.valueOf(days) + String.valueOf(time);
        return Bytes.toBytes(keystr);

    }

    public static byte[] getRowkey(String[] params, String[] types) {
        StringBuilder keysb = new StringBuilder();
        byte[] rowkey_low = null;
        for (int i = 0; i < params.length; i++) {
            if (i == 0) {
                keysb.append(params[i]);
                rowkey_low = object2Bytes(params[i], types[i]);
            } else {
                keysb.append("~").append(params[i]);
                rowkey_low = Bytes.add(rowkey_low, object2Bytes(params[i], types[i]));
            }

        }
        String low = keysb.toString();
        short high = getRowkeyHigh(low, 3);
        //      System.out.println(high);
        byte[] rowkey = Bytes.add(Bytes.toBytes(high), Bytes.toBytes(low));
//        System.out.println(Bytes.toShort(Bytes.head(rowkey,2)) + Bytes.toString(Bytes.tail(rowkey,rowkey.length-2)));
        return rowkey;
    }

    /**
     * 获取rowkey高位
     *
     * @param key_low:  rowkey低位字符串
     * @param highByte: 高位位数
     * @return
     */
    private static short getRowkeyHigh(String key_low, int highByte) {
        //取绝对值,去除负号
        String strHead1 = String.valueOf(Math.abs(key_low.hashCode()));
        //截取后highByte位
        String strHead = strHead1.length() < highByte ? strHead1 : strHead1.substring(strHead1.length() - highByte);
        int strlen = strHead.length();
        StringBuilder sb = new StringBuilder();
        //反转后四位
        while (strlen > 0) {
            sb.append(strHead.charAt(strlen - 1));
            strlen--;
        }
        return Short.parseShort(sb.toString());
    }

    public static byte[] getRowkey(LinkedHashMap<String, String> params) {
        StringBuilder keysb = new StringBuilder();
        byte[] rowkey_low = null;
        int i = 0;
        for (Map.Entry col : params.entrySet()) {
            if (i == 0) {
                keysb.append(col.getKey());
                rowkey_low = object2Bytes(col.getKey(), col.getValue().toString());
            } else {
                keysb.append("~").append(col.getKey());
                rowkey_low = Bytes.add(rowkey_low, object2Bytes(col.getKey(), col.getValue().toString()));
            }
            i++;
        }
        String low = keysb.toString();
        short high = getRowkeyHigh(low, 3);
        //      System.out.println(high);
        byte[] rowkey = Bytes.add(Bytes.toBytes(high), rowkey_low);
//        System.out.println(Bytes.toShort(Bytes.head(rowkey,2)) + Bytes.toString(Bytes.tail(rowkey,rowkey.length-2)));
        return rowkey;
    }

    public static LinkedHashMap<String, String> sqlServerDataTypeToPhoenix(JSONObject columnMap) {
        LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
        for (Map.Entry entry : columnMap.entrySet()) {
            String key = entry.getKey().toString();
            String val = entry.getValue().toString();
            String trunsval = "";
            switch (val) {
                case "int":
                    trunsval = "INTEGER";
                    break;
                case "numeric":
                    trunsval = "DECIMAL";
                    break;
                case "money":
                    trunsval = "DOUBLE";
                    break;
                case "datetime":
                    trunsval = "TIMESTAMP";
                    break;
                case "smalldatetime":
                    trunsval = "DATE";
                    break;
                case "char":
                case "nchar":
                case "varchar":
                case "nvarchar":
                case "text":
                case "ntext":
                    trunsval = "VARCHAR";
                    break;
                case "bit":
                    trunsval = "BOOLEAN";
                    break;
                default:
                    trunsval = val.toUpperCase();
                    break;

            }
            map.put(key, trunsval);
        }
        return map;
    }

    /**
     * 数据转换
     *
     * @param val:值
     * @param columnType:数据类型
     * @return: 二进制数据
     */
    public static byte[] object2Bytes(Object val, String columnType) {
        if (val == null) {
            return null;
        }
        switch (columnType) {
            case "bigint":
                return Bytes.toBytes(Long.parseLong(val.toString()));
            case "int":
                return Bytes.toBytes(Integer.parseInt(val.toString()));
            case "short":
                return Bytes.toBytes(Short.parseShort(val.toString()));
            case "tiny":
                return Bytes.toBytes(Byte.parseByte(val.toString()));
            case "bit":
                return Bytes.toBytes(Boolean.parseBoolean(val.toString()));
            case "float":
                return Bytes.toBytes(Float.parseFloat(val.toString()));
            case "double":
            case "money":
                return Bytes.toBytes(Double.parseDouble(val.toString()));
            case "numeric":
            case "decimal":
                return Bytes.toBytes(new BigDecimal(val.toString()));
            default:
                return Bytes.toBytes(val.toString());
        }
    }
    /**
     * 数据转换
     *
     * @param val:值
     * @param columnType:数据类型
     * @return: 二进制数据
     */
    public static byte[] object2PhoenixBytes(Object val, String columnType) {
        if (val == null) {
            return null;
        }
        switch (columnType) {
            case "bigint":
                return PLong.INSTANCE.toBytes(Long.parseLong(val.toString()));
            case "int":
                return PInteger.INSTANCE.toBytes(Integer.parseInt(val.toString()));
            case "short":
                return PSmallint.INSTANCE.toBytes(Short.parseShort(val.toString()));
            case "tiny":
                return PTinyint.INSTANCE.toBytes(Byte.parseByte(val.toString()));
            case "bit":
                return PBoolean.INSTANCE.toBytes(Boolean.parseBoolean(val.toString()));
            case "float":
                return PFloat.INSTANCE.toBytes(Float.parseFloat(val.toString()));
            case "double":
            case "money":
                return PDouble.INSTANCE.toBytes(Double.parseDouble(val.toString()));
            case "numeric":
            case "decimal":
                return PDecimal.INSTANCE.toBytes(new BigDecimal(val.toString()));
            default:
                return Bytes.toBytes(val.toString());
        }
    }
}