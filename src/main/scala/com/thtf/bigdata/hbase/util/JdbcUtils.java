package com.thtf.bigdata.hbase.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.thtf.bigdata.util.PropertiesUtils;

/**
 * User: ieayoio
 * Date: 18-7-9
 */
public class JdbcUtils {
    public static final String PHOENIX_DRIVER_CLASS_NAME = PropertiesUtils.getPropertiesByKey("driver.phoenix");
    public static final String PHOENIX_URL = PropertiesUtils.getPropertiesByKey("url.phoenix");

    public static final String MYSQL_DRIVER_CLASS_NAME = PropertiesUtils.getPropertiesByKey("driver.mysql");
    public static final String MYSQL_URL = PropertiesUtils.getPropertiesByKey("url.mysql");
    public static final String MYSQL_PARAM_URL = PropertiesUtils.getPropertiesByKey("url.mysql.param");

    public static LinkedHashMap<String, Object> getObjectFromResultSet(ResultSet resultSet) throws SQLException {
        int columnCount = resultSet.getMetaData().getColumnCount();

        LinkedHashMap<String, Object> map = new LinkedHashMap<>();

        for (int j = 1; j <= columnCount; j++) {
            String columnName = resultSet.getMetaData().getColumnName(j);
            int columnType = resultSet.getMetaData().getColumnType(j);

            switch (columnType) {
                case Types.VARCHAR:
                    map.put(columnName, resultSet.getString(columnName));
                    break;
                case Types.INTEGER:
                    map.put(columnName, resultSet.getInt(columnName));
                    break;
                case Types.SMALLINT:
                    map.put(columnName, resultSet.getShort(columnName));
                    break;
                case Types.TINYINT:
                    map.put(columnName, resultSet.getByte(columnName));
                    break;
                case Types.BIGINT:
                    map.put(columnName, resultSet.getLong(columnName));
                    break;
                case Types.FLOAT:
                    map.put(columnName, resultSet.getFloat(columnName));
                    break;
                case Types.DOUBLE:
                    map.put(columnName, resultSet.getDouble(columnName));
                    break;
                case Types.BOOLEAN:
                    map.put(columnName, resultSet.getBoolean(columnName));
                    break;
                case Types.DECIMAL:
                    map.put(columnName, resultSet.getBigDecimal(columnName));
                    break;
                case Types.VARBINARY:
                    map.put(columnName, Bytes.toDouble(resultSet.getBytes(columnName)));
                    break;
                default:
                    map.put(columnName, resultSet.getString(columnName));
            }
        }
        return map;
    }
}
