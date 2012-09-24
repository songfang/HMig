package com.tongji.hbase.migration.common;

import java.sql.SQLException;

/**
 * Exception thrown when error occurs while processing HSQL.
 * 
 * @author Zhao Long
 */
public class HSQLException extends SQLException {

	/**
	 * Constructor.
	 * 
	 * @param reason
	 *            the error reason
	 */
	public HSQLException(String reason) {
		super(reason);
	}

	/**
	 * Constructor.
	 * 
	 * @param reason
	 *            the error reason
	 * @param cause
	 *            the cause
	 */
	public HSQLException(String reason, Throwable cause) {
		super(reason, cause);
	}
}
