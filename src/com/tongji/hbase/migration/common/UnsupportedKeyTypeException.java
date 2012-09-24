package com.tongji.hbase.migration.common;

/**
 * Exception thrown when the given type is unsupported.
 * 
 * @author Zhao Long
 */
public class UnsupportedKeyTypeException extends Exception {

	/**
	 * Constructor.
	 * 
	 * @param typeName
	 *            the type name
	 */
	public UnsupportedKeyTypeException(String typeName) {
		super("Unsupported Key Type: " + typeName);
	}
}
