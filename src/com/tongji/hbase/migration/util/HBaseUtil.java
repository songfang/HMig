package com.tongji.hbase.migration.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.tongji.hbase.migration.common.Config;
import com.tongji.hbase.migration.common.ConfigDefs;

/**
 * Tool to operate HBase.
 * 
 * @author Zhao Long
 */
public class HBaseUtil implements ConfigDefs {

	private static final String quorum = Config.getProperties().getProperty(
			PROPERTY_HBASE_ZOOKEEPER_QUORUM);

	/**
	 * Get the configuration of HBase.
	 * 
	 * @return the configuration
	 */
	public static Configuration getConfig() {
		Configuration conf = HBaseConfiguration.create();
		conf.set(PROPERTY_HBASE_ZOOKEEPER_QUORUM, quorum);
		return conf;
	}

	/**
	 * Get the admin of HBase.
	 * 
	 * @return the admin
	 * @throws IOException
	 *             when failed to connect to HBase
	 */
	public static HBaseAdmin getAdmin() throws IOException {
		Configuration conf = getConfig();
		return new HBaseAdmin(conf);
	}

	/**
	 * Create HTable.
	 * 
	 * @param admin
	 *            the admin
	 * @param tableName
	 *            the table name
	 * @param columnFamilyNames
	 *            the column family names
	 * @throws IOException
	 *             when failed to create table
	 */
	public static void creatHTable(HBaseAdmin admin, String tableName,
			Collection<String> columnFamilyNames) throws IOException {
		HTableDescriptor htd = new HTableDescriptor(tableName);
		for (String cfn : columnFamilyNames) {
			HColumnDescriptor hcd = new HColumnDescriptor(cfn);
			htd.addFamily(hcd);
		}
		admin.createTable(htd);
	}

	/**
	 * Get HTable by the table name.
	 * 
	 * @param tableName
	 *            the table name
	 * @return the HTable
	 * @throws IOException
	 *             when failed to get HTable
	 */
	public static HTable getHTable(String tableName) throws IOException {
		return new HTable(getConfig(), tableName);
	}

	/**
	 * Delete HTable.
	 * 
	 * @param admin
	 *            the admin
	 * @param tableName
	 *            the table name
	 * @throws IOException
	 *             when failed to delete table
	 */
	public static void deleteHTable(HBaseAdmin admin, String tableName)
			throws IOException {
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
	}

	/**
	 * Get value from Result.
	 * 
	 * @param result
	 *            the Result
	 * @param columnFamilyName
	 *            the column family name
	 * @param columnName
	 *            the column name
	 * @return the bytes vale
	 */
	public static byte[] getResultValue(Result result, String columnFamilyName,
			String columnName) {
		return getResultValue(result, columnFamilyName,
				Bytes.toBytes(columnName));
	}

	/**
	 * Get value from Result.
	 * 
	 * @param result
	 *            the Result
	 * @param columnFamilyName
	 *            the column family name
	 * @param columnName
	 *            the column name
	 * @return the bytes vale
	 */
	public static byte[] getResultValue(Result result, String columnFamilyName,
			byte[] columnName) {
		return result.getValue(Bytes.toBytes(columnFamilyName), columnName);
	}

	/**
	 * Add column family to Scan.
	 * 
	 * @param scan
	 *            the Scan
	 * @param columnFamilyName
	 *            the column family name
	 * @return the Scan
	 */
	public static Scan addScanFamily(Scan scan, String columnFamilyName) {
		scan.addFamily(Bytes.toBytes(columnFamilyName));
		return scan;
	}

	/**
	 * Add column to Scan.
	 * 
	 * @param scan
	 *            the Scan
	 * @param columnFamilyName
	 *            the column family name
	 * @param columnName
	 *            the column name
	 * @return the Scan
	 */
	public static Scan addScanColumn(Scan scan, String columnFamilyName,
			String columnName) {
		scan.addColumn(Bytes.toBytes(columnFamilyName),
				Bytes.toBytes(columnName));
		return scan;
	}

	/**
	 * Add column family to Get.
	 * 
	 * @param get
	 *            the Get
	 * @param columnFamilyName
	 *            the column family name
	 * @return the Get
	 */
	public static Get addGetFamily(Get get, String columnFamilyName) {
		get.addFamily(Bytes.toBytes(columnFamilyName));
		return get;
	}

	/**
	 * Add column to Get.
	 * 
	 * @param get
	 *            the Get
	 * @param columnFamilyName
	 *            the column family name
	 * @param columnName
	 *            the column name
	 * @return the Get
	 */
	public static Get addGetColumn(Get get, String columnFamilyName,
			String columnName) {
		get.addColumn(Bytes.toBytes(columnFamilyName),
				Bytes.toBytes(columnName));
		return get;
	}

	/**
	 * Get value from bytes according to given Class.
	 * 
	 * @param clazz
	 *            the Class
	 * @param val
	 *            the bytes value
	 * @return the value
	 * @throws IOException
	 *             when the class type is unsupported
	 */
	public static <T> T toValue(Class<T> clazz, byte[] val) throws IOException {
		Object o = null;
		if (String.class.equals(clazz)) {
			o = Bytes.toString(val);
		} else if (Integer.class.equals(clazz)) {
			o = Bytes.toInt(val);
		} else if (Double.class.equals(clazz)) {
			o = Bytes.toDouble(val);
		} else if (Boolean.class.equals(clazz)) {
			o = Bytes.toBoolean(val);
		} else if (Date.class.equals(clazz)) {
			Long time = Bytes.toLong(val);
			o = new Date(time);
		} else {
			throw new IOException("Unsupported Class Type to Get from Bytes: "
					+ clazz.getSimpleName());
		}

		return clazz.cast(o);
	}

	/**
	 * Convert value into bytes according to given Class.
	 * 
	 * @param clazz
	 *            the Class
	 * @param val
	 *            the value
	 * @return the bytes
	 * @throws IOException
	 *             when the class type is unsupported
	 */
	public static byte[] toBytes(Class<?> clazz, Object val) throws IOException {
		if (String.class.equals(clazz)) {
			return Bytes.toBytes((String) val);
		} else if (Integer.class.equals(clazz)) {
			return Bytes.toBytes((Integer) val);
		} else if (Double.class.equals(clazz)) {
			return Bytes.toBytes((Double) val);
		} else if (Boolean.class.equals(clazz)) {
			return Bytes.toBytes((Boolean) val);
		} else if (Date.class.equals(clazz)) {
			Date date = (Date) val;
			return Bytes.toBytes(date.getTime());
		} else {
			throw new IOException(
					"Unsupported Class Type to Convert into Bytes: "
							+ clazz.getSimpleName());
		}
	}

	/**
	 * Close the ResultScanner.
	 * 
	 * @param connection
	 *            the ResultScanner
	 */
	public static void close(ResultScanner rs) {
		if (rs != null) {
			rs.close();
		}
		rs = null;
	}
}
