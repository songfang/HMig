package com.tongji.hbase.migration.hsql;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.tongji.hbase.migration.common.HSQLException;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * The basic inner result return by the HSQL.
 * 
 * @author Zhao Long
 */
public class HSQLInnerResult {

	protected Map<String, byte[]> valueMap;

	/**
	 * Constructor. <b>Do NOT call manually.</b>
	 */
	public HSQLInnerResult() {
		this.valueMap = new HashMap<String, byte[]>();
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

	// Get value in Result.
	private <T> T getValue(Class<T> clazz, String columnName)
			throws HSQLException {
		byte[] val = this.valueMap.get(columnName);
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
