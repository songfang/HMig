package com.tongji.hbase.migration.hsql;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.hbase.client.Result;

import com.tongji.hbase.migration.common.HSQLException;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * The basic result return by the HSQL.
 * 
 * @author Zhao Long
 */
public class HSQLResult {

	private Result result;
	private String tableName;

	/**
	 * Constructor. <b>Do NOT call manually.</b>
	 * 
	 * @param tableName
	 *            the table name
	 * @param result
	 *            the Result
	 */
	public HSQLResult(String tableName, Result result) {
		this.tableName = tableName;
		this.result = result;
	}

	/**
	 * Get String value of the column.
	 * 
	 * @param columnName
	 *            the column name
	 * @return the value
	 * @throws HSQLException
	 *             when failed to get value
	 */
	public String getString(String columnName) throws HSQLException {
		return getValue(String.class, columnName);
	}

	/**
	 * Get Integer value of the column.
	 * 
	 * @param columnName
	 *            the column name
	 * @return the value
	 * @throws HSQLException
	 *             when failed to get value
	 */
	public int getInt(String columnName) throws HSQLException {
		return getValue(Integer.class, columnName);
	}

	/**
	 * Get Double value of the column.
	 * 
	 * @param columnName
	 *            the column name
	 * @return the value
	 * @throws HSQLException
	 *             when failed to get value
	 */
	public double getDouble(String columnName) throws HSQLException {
		return getValue(Double.class, columnName);
	}

	/**
	 * Get Boolean value of the column.
	 * 
	 * @param columnName
	 *            the column name
	 * @return the value
	 * @throws HSQLException
	 *             when failed to get value
	 */
	public boolean getBoolean(String columnName) throws HSQLException {
		return getValue(Boolean.class, columnName);
	}

	/**
	 * Get Date value of the column.
	 * 
	 * @param columnName
	 *            the column name
	 * @return the value
	 * @throws HSQLException
	 *             when failed to get value
	 */
	public Date getDate(String columnName) throws HSQLException {
		return getValue(Date.class, columnName);
	}

	/**
	 * Execute HSQL query.
	 * 
	 * @param hsql
	 *            the HSQL
	 * @return the scanner
	 * @throws HSQLException
	 *             when error occurs
	 */
	public HSQLInnerScanner executeQuery(String hsql) throws HSQLException {
		return HSQLConnection.executeInner(HSQLConnection.queryInnerExecutors,
				result, this.tableName, hsql);
	}

	// Get value in Result.
	private <T> T getValue(Class<T> clazz, String columnName)
			throws HSQLException {
		byte[] val = HBaseUtil.getResultValue(result, tableName, columnName);
		if (val == null) {
			throw new HSQLException("Empty Value of: " + columnName);
		}
		try {
			return HBaseUtil.toValue(clazz, val);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new HSQLException("Failed to Get Value of :" + columnName, e);
		}
	}
}
