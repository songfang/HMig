package com.tongji.hbase.migration.common;

/**
 * Exception thrown when the given table name is unknown.
 * 
 * @author Zhao Long
 */
public class UnknownTableException extends Exception {

	/**
	 * Constructor.
	 * 
	 * @param tableName
	 *            the table name
	 */
	public UnknownTableException(String tableName) {
		super("Unknown Table: " + tableName);
	}
}
