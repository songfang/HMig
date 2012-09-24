package com.tongji.hbase.migration.common;

/**
 * Exception thrown when the given key is unknown.
 * 
 * @author Zhao Long
 */
public class UnknownKeyException extends Exception {

	/**
	 * Constructor.
	 * 
	 * @param keyName
	 *            the key name
	 */
	public UnknownKeyException(String keyName) {
		super("Unknown Key: " + keyName);
	}
}
