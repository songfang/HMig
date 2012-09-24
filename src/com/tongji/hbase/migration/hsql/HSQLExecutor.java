package com.tongji.hbase.migration.hsql;

import com.tongji.hbase.migration.common.HSQLException;

/**
 * Interface to execute HSQL.
 * 
 * @author Zhao Long
 */
public interface HSQLExecutor<T> extends HSQLCommon {

	/**
	 * Execute the HSQL.
	 * 
	 * @param hsql
	 *            the HSQL
	 * @param tableNames
	 *            the table names
	 * @return the object
	 * @throws HSQLException
	 *             when error occurs
	 */
	public T execute(String hsql, String[] tableNames) throws HSQLException;
}
