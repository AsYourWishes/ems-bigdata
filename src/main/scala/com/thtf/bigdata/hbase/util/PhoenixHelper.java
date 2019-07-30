package com.thtf.bigdata.hbase.util;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONArray;
import com.thtf.bigdata.util.PropertiesUtils;

/**
 * phoenix相关操作
 * 
 * @author gk-dingml
 */
public class PhoenixHelper {
	private static Logger log = Logger.getLogger(PhoenixHelper.class);

	/**
	 * 单条数据写入(更新/插入)
	 * 
	 * @param connection:数据库连接
	 * @param tableName:表名称
	 * @param data:数据list
	 * @param dataType:数据类型list
	 * @return
	 */
	public static boolean upsert(Connection connection, String tableName, List<Object> data, List<String> dataType) {
		// 拼sql语句
		String sql = getSql(tableName, dataType);
		// 内部处理异常
		try (PreparedStatement pst = connection.prepareStatement(sql)) {
			// 赋值
			for (int i = 0; i < data.size(); i++) {
				setPrepared(pst, i + 1, data.get(i), dataType.get(i));// 根据类型赋值
			}
			// 预提交
			pst.executeUpdate();
			// 提交写入请求
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 批量数据写入(更新/插入)
	 * 
	 * @param nameSpace:命名空间
	 * @param tableName:表名称
	 * @param data:数据list---二维数组
	 * @param dataType:数据类型list
	 * @return
	 * @throws SQLException
	 * @throws InterruptedException 
	 */
	public static boolean upsertList(String nameSpace, String tableName, List<JSONArray> data, List<String> dataType)
			throws SQLException, InterruptedException {
		// 获取数据库连接
		Connection connection = getConnection(nameSpace);
		// 拼sql
		String sql = getSql(tableName, dataType);
		// 预连接
		PreparedStatement pst = connection.prepareStatement(sql);
		int size = data.size();
		// 获取时间戳
		long l = System.currentTimeMillis();
		// 外循环,每次取一条记录
		for (int i = 0; i < size; i++) {
			JSONArray row = data.get(i);
			// 内循环,取记录中所有列
			for (int j = 0; j < dataType.size(); j++) {
				setPrepared(pst, j + 1, row.get(j), dataType.get(j));// 根据类型赋值
			}
			// 每条记录赋值后预提交一次
			pst.executeUpdate();

			// 读取设置的预提交数量大小
			String prepareSizeStr = PropertiesUtils.getPropertiesByKey("phoenix.prepare.commit.size");
			int prepareSize = prepareSizeStr == null ? 0 : Integer.parseInt(prepareSizeStr);
			if (prepareSize != 0) {
				if ((i + 1) % prepareSize == 0) {
					// 提交写入请求
					connection.commit();
					// 清除批处理命令
					// pst.clearBatch();
					// 清理预提交参数命令
					// pst.clearParameters();
					System.out.println("预提交数据大小为：" + prepareSize + ",耗时：" + (System.currentTimeMillis() - l) + "ms");
					log.info("预提交数据大小为：" + prepareSize + ",耗时：" + (System.currentTimeMillis() - l) + "ms");
					l = System.currentTimeMillis();
				}
			}
		}
		// 提交写入请求
		connection.commit();
		// 关闭连接
		connection.close();
		return true;
	}

	/**
	 * 拼sql语句
	 * 
	 * @param tableName:表名
	 * @param dataType:列数组
	 * @return
	 */
	private static String getSql(String tableName, List<String> dataType) {
		StringBuilder sql = new StringBuilder();
		sql.append("UPSERT INTO \"").append(tableName).append("\" VALUES (");
		// PK,NAME,AGE) VALUES (?,?,?)";
		for (int i = 0; i < dataType.size(); i++) {
			if (i > 0)
				sql.append(",");
			sql.append("?");
		}
		sql.append(")");
		return sql.toString();
	}

	/**
	 * 查询数据
	 * 
	 * @param nameSpace:命名空间
	 * @param tableName:表名
	 * @param selects:选择列
	 * @param wheres:查询条件
	 * @return
	 * @throws InterruptedException 
	 */
	public static ResultSet query(String nameSpace, String tableName, String[] selects, String[] wheres) throws InterruptedException {
		ResultSet rs = null;
		try {
			Connection conn = getConnection(nameSpace);
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT ");
			if (selects != null && selects.length >= 0) {
				sb.append("\"").append(String.join("\",\"", selects)).append("\"");
			} else {
				sb.append("*");
			}
			sb.append(" FROM \"").append(tableName).append("\"");
			if (wheres != null && wheres.length > 0) {
				sb.append(" WHERE ");
				for (int i = 0; i < wheres.length; i++) {
					if (i > 0)
						sb.append(" AND ");
					sb.append(wheres[i]);
				}
			}
			String sql = sb.toString();
			PreparedStatement pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			log.error(e.toString());
			rs = null;
		}
		return rs;
	}

	/**
	 * 根据数据类型给sql控件传参
	 * 
	 * @param pst:PreparedStatement控件
	 * @param index:参数位置
	 * @param data:数据
	 * @param type:类型
	 * @throws SQLException
	 */
	private static void setPrepared(PreparedStatement pst, int index, Object data, String type) throws SQLException {
		switch (type) {
		case "bigint":
			pst.setLong(index, Long.parseLong(data.toString()));
			break;
		case "int":
		case "smallint":
			pst.setInt(index, Integer.parseInt(data.toString()));
			break;
		case "short":
		case "tiny":
			pst.setShort(index, Byte.parseByte(data.toString()));
			break;
		case "bit":
			pst.setBoolean(index, Boolean.parseBoolean(data.toString()));
			break;
		case "float":
			pst.setFloat(index, Float.parseFloat(data.toString()));
			break;
		case "double":
		case "money":
			pst.setDouble(index, Double.parseDouble(data.toString()));
			break;
		case "numeric":
		case "decimal":
			pst.setBigDecimal(index, new BigDecimal(data.toString()));
			// return Bytes.toBytes();
			break;
		// phoenix 直接将字符串转换为datetime有时区差异问题
		case "datetime":
			pst.setTimestamp(index, Timestamp.valueOf(data.toString()));
			break;
		default:
			if (data == null) {
				pst.setString(index, null);
			} else {
				pst.setString(index, data.toString());
			}
			break;
		}
	}

	/**
	 * 获取数据连接
	 * 
	 * @param nameSpace:命名空间
	 * @return
	 * @throws SQLException
	 * @throws InterruptedException 
	 */
	public static Connection getConnection(String nameSpace) throws SQLException, InterruptedException {
		long l = System.currentTimeMillis();
		Properties props = new Properties();
		props.setProperty("phoenix.functions.allowUserDefinedFunctions", "true");
		props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
		props.setProperty("phoenix.query.dateFormatTimeZone", "Asia/Shanghai");
		props.setProperty("phoenix.connection.schema", nameSpace);
		
		int attemptCount = Integer.parseInt(PropertiesUtils.getPropertiesByKey("phoenix.attempt.count"));
		int attemptTime = Integer.parseInt(PropertiesUtils.getPropertiesByKey("phoenix.attempt.time"));
		Connection connection = null;
		while (attemptCount > 0 && connection == null) {
			try {
				connection = DriverManager.getConnection(PropertiesUtils.getPropertiesByKey("phoenix.url"), props);
				connection.setAutoCommit(false);
				System.out.println("链接时间：" + (System.currentTimeMillis() - l));
				attemptCount = Integer.parseInt(PropertiesUtils.getPropertiesByKey("phoenix.attempt.count"));
			} catch (Exception e) {
				Thread.sleep(attemptTime * 1000);
				attemptCount--;
			}
		}
		if(connection == null) {
			throw new RuntimeException("获取连接失败，尝试次数"+attemptCount+"次，尝试间隔时间"+attemptTime+"秒");
		}
		return connection;
	}
}
