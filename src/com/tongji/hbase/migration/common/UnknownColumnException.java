package com.tongji.hbase.migration.common;

/**
 * Exception thrown when the given column is unknown.
 * 
 * @author Zhao Long
 */
public class UnknownColumnException extends Exception {

	/**
	 * Constructor.
	 * 
	 * @param columnName
	 *            the column name
	 */
	public UnknownColumnException(String columnName) {
		super("Unknown Column: " + columnName);
	}
}