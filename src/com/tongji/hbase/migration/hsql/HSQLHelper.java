package com.tongji.hbase.migration.hsql;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.tongji.hbase.migration.common.HSQLException;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * Tool to build basic information of tables in HBase.
 * 
 * @author Zhao Long
 */
public class HSQLHelper implements HSQLCommon {

	/**
	 * The sub column separator.
	 */
	public static final String COLUMN_SEPARATOR = "@";

	/**
	 * Build row key according to HTableDef
	 * 
	 * @param htd
	 *            the HTableDef
	 * @param val
	 *            the value of row key
	 * @return the row key in bytes
	 * @throws HSQLException
	 *             when failed to build row key
	 */
	public static byte[] buildRowKey(HTableDef htd, Object val)
			throws HSQLException {
		try {
			Class<?> clazz = htd.getRowKey().getColumnType().getTypeClass();
			return HBaseUtil.toBytes(clazz, val);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new HSQLException("Failed to Build Row Key of Table: "
					+ htd.getTableName(), e);
		}
	}

	/**
	 * Add column value into Put.
	 * 
	 * @param put
	 *            the Put
	 * @param htd
	 *            the HTableDef
	 * @param columnFamilyName
	 *            the column family name
	 * @param columnName
	 *            the column name
	 * @param val
	 *            the value
	 * @return the Put
	 * @throws HSQLException
	 *             when failed to add value into Put
	 */
	public static Put addColumn(Put put, HTableDef htd,
			String columnFamilyName, String columnName, Object val)
			throws HSQLException {
		try {
			Class<?> clazz = htd.getColumn(columnFamilyName, columnName)
					.getColumnType().getTypeClass();
			byte[] byteVal = HBaseUtil.toBytes(clazz, val);
			put.add(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName),
					byteVal);
			return put;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new HSQLException("Failed to Add Column to Put of Table: "
					+ htd.getTableName(), e);
		}
	}

	/**
	 * Add sub column value into Put.
	 * 
	 * @param put
	 *            the Put
	 * @param htd
	 *            the HTableDef
	 * @param columnId
	 *            the column id
	 * @param columnFamilyName
	 *            the column family name
	 * @param columnName
	 *            the column name
	 * @param val
	 *            the value
	 * @return the Put
	 * @throws HSQLException
	 *             when failed to add value into Put
	 */
	public static Put addSubColumn(Put put, HTableDef htd,
			String columnFamilyName, String columnName, Object columnId,
			Object val) throws HSQLException {
		try {
			Class<?> clazz = htd.getColumn(columnFamilyName, columnName)
					.getColumnType().getTypeClass();
			byte[] byteVal = HBaseUtil.toBytes(clazz, val);

			clazz = htd.getRowKey().getColumnType().getTypeClass();
			byte[] byteId = HBaseUtil.toBytes(clazz, columnId);
			byte[] byteName = Bytes.toBytes(columnName);

			put.add(Bytes.toBytes(columnFamilyName),
					buildSubColumnName(byteId, byteName), byteVal);
			return put;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new HSQLException(
					"Failed to Add Sub Column to Put of Table: "
							+ htd.getTableName(), e);
		}
	}

	/**
	 * Build sub column name.
	 * 
	 * @param columnId
	 *            the column id
	 * @param columnName
	 *            the column name
	 * @return the sub column name
	 */
	public static byte[] buildSubColumnName(byte[] columnId, byte[] columnName) {
		return Bytes.add(columnName, Bytes.toBytes(COLUMN_SEPARATOR), columnId);
	}

	/**
	 * Build sub column name.
	 * 
	 * @param columnId
	 *            the column id
	 * @param columnName
	 *            the column name
	 * @return the sub column name
	 */
	public static byte[] buildSubColumnName(byte[] columnId, String columnName) {
		return buildSubColumnName(columnId, Bytes.toBytes(columnName));
	}

	/**
	 * Get the id in sub column.
	 * 
	 * @param subColumn
	 *            the sub column
	 * @return the column id
	 */
	public static byte[] getSubColumnId(byte[] subColumn) {
		String val = Bytes.toStringBinary(subColumn);
		return Bytes.toBytesBinary(val.split(COLUMN_SEPARATOR)[1]);
	}

	/**
	 * Get the name in sub column.
	 * 
	 * @param subColumn
	 *            the sub column
	 * @return the column name
	 */
	public static String getSubColumnName(byte[] subColumn) {
		String val = Bytes.toStringBinary(subColumn);
		return val.split(COLUMN_SEPARATOR)[0];
	}

	/**
	 * Format HSQL.
	 * 
	 * @param hsql
	 *            the String
	 * @return the formatted String
	 */
	public static String formatHSQL(String hsql) {
		hsql = hsql.replaceAll("\\s+", HSQL_SYM_SPACE);
		hsql = hsql.replaceAll("\\s?=\\s?", HSQL_SYM_EQUAL);
		hsql = hsql.replaceAll("\\s?,\\s?", HSQL_SYM_COMMA);
		return hsql.trim().toUpperCase();
	}
}
