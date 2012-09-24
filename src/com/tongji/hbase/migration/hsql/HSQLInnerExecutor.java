package com.tongji.hbase.migration.hsql;

import org.apache.hadoop.hbase.client.Result;

import com.tongji.hbase.migration.common.HSQLException;

/**
 * Interface to execute inner HSQL.
 * 
 * @author Zhao Long
 */
public interface HSQLInnerExecutor<T> extends HSQLCommon {

	/**
	 * Execute the HSQL.
	 * 
	 * @param result
	 *            the result object
	 * @param mainTableName
	 *            the main table name
	 * @param hsql
	 *            the HSQL
	 * @return the object
	 * @throws HSQLException
	 *             when error occurs
	 */
	public T execute(Result result, String mainTableName, String hsql)
			throws HSQLException;
}
