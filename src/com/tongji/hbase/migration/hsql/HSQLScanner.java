package com.tongji.hbase.migration.hsql;

import java.io.Closeable;

import com.tongji.hbase.migration.common.HSQLException;

/**
 * Interface of scanner.
 * 
 * @author Zhao Long
 */
public interface HSQLScanner extends Closeable, Iterable<HSQLResult> {

	/**
	 * Grab the next result's worth of values.
	 * 
	 * @return the next HSQLResult
	 * @throws HSQLException
	 *             when error occurs
	 */
	public HSQLResult next() throws HSQLException;

	@Override
	public void close();
}
