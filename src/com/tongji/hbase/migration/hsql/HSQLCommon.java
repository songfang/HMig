package com.tongji.hbase.migration.hsql;

import com.tongji.hbase.migration.Adapter;

/**
 * The common definitions of HSQL.
 * 
 * @author Zhao Long
 */
public interface HSQLCommon {
	/**
	 * Key words.
	 */
	public static final String HSQL_WORD_FROM = "FROM";
	public static final String HSQL_WORD_SELECT = "SELECT";
	public static final String HSQL_WORD_WHERE = "WHERE";

	/**
	 * Symbols.
	 */
	public static final String HSQL_SYM_SPACE = " ";
	public static final String HSQL_SYM_EQUAL = "=";
	public static final String HSQL_SYM_COMMA = ",";

	/**
	 * Formats.
	 */
	public static final String HSQL_FORMAT_DATE = "yyyy-MM-dd HH:mm:ss";

	/**
	 * Common adapter.
	 */
	public static final Adapter adapter = Adapter.getInstance();
}
