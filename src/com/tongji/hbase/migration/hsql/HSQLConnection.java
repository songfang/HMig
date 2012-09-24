package com.tongji.hbase.migration.hsql;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;

import com.tongji.hbase.migration.common.Config;
import com.tongji.hbase.migration.common.ConfigDefs;
import com.tongji.hbase.migration.common.HSQLException;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * Tool to connect and execute HSQL.
 * 
 * @author Zhao Long
 */
public class HSQLConnection implements HSQLCommon, ConfigDefs {

	protected static final HTablePool pool;
	protected static final Map<String, Class<? extends HSQLExecutor<HSQLScanner>>> queryExecutors;
	protected static final Map<String, Class<? extends HSQLExecutor<HSQLResult>>> getExecutors;
	protected static final Map<String, Class<? extends HSQLInnerExecutor<HSQLInnerScanner>>> queryInnerExecutors;

	static {
		queryExecutors = new HashMap<String, Class<? extends HSQLExecutor<HSQLScanner>>>();
		getExecutors = new HashMap<String, Class<? extends HSQLExecutor<HSQLResult>>>();
		queryInnerExecutors = new HashMap<String, Class<? extends HSQLInnerExecutor<HSQLInnerScanner>>>();

		int maxSize = Config.getProperties().getIntProperty(
				PROPERTY_HBASE_POOL_SIZE);
		pool = new HTablePool(HBaseUtil.getConfig(), maxSize);

		// Reload all parsers.
		Config.getProperties().getAllClasses(PROPERTY_HSQL_EXECUTOR_CLASS);
	}

	/**
	 * Register the query executor.
	 * 
	 * @param regex
	 *            the expression to executor
	 * @param clazz
	 *            the executor Class
	 */
	public static void registerQueryExecutor(String regex,
			Class<? extends HSQLExecutor<HSQLScanner>> clazz) {
		queryExecutors.put(regex, clazz);
	}

	/**
	 * Register the query inner executor.
	 * 
	 * @param regex
	 *            the expression to executor
	 * @param clazz
	 *            the executor Class
	 */
	public static void registerQueryInnerExecutor(String regex,
			Class<? extends HSQLInnerExecutor<HSQLInnerScanner>> clazz) {
		queryInnerExecutors.put(regex, clazz);
	}

	/**
	 * Register the Get executor.
	 * 
	 * @param regex
	 *            the expression to executor
	 * @param clazz
	 *            the executor Class
	 */
	public static void registerGetExecutor(String regex,
			Class<? extends HSQLExecutor<HSQLResult>> clazz) {
		getExecutors.put(regex, clazz);
	}

	/**
	 * Execute HSQL query.
	 * 
	 * @param hsql
	 *            the HSQL
	 * @return the scanner
	 * @throws HSQLException
	 *             when error occurs
	 */
	public HSQLScanner executeQuery(String hsql) throws HSQLException {
		return execute(queryExecutors, hsql, new String[] {});
	}

	/**
	 * Execute HSQL query.
	 * 
	 * @param hsql
	 *            the HSQL
	 * @param tableNames
	 *            the table names
	 * @return the scanner
	 * @throws HSQLException
	 *             when error occurs
	 */
	public HSQLScanner executeQuery(String hsql, String[] tableNames)
			throws HSQLException {
		return execute(queryExecutors, hsql, tableNames);
	}

	/**
	 * Execute HSQL get with one result.
	 * 
	 * @param hsql
	 *            the HSQL
	 * @param tableNames
	 *            the table names
	 * @return the scanner
	 * @throws HSQLException
	 *             when error occurs
	 */
	public HSQLResult executeGet(String hsql, String[] tableNames)
			throws HSQLException {
		return execute(getExecutors, hsql, tableNames);
	}

	/**
	 * Execute HSQL get with one result.
	 * 
	 * @param hsql
	 *            the HSQL
	 * @return the scanner
	 * @throws HSQLException
	 *             when error occurs
	 */
	public HSQLResult executeGet(String hsql) throws HSQLException {
		return execute(getExecutors, hsql, new String[] {});
	}

	/**
	 * Get connection by the given table name.
	 * 
	 * @param tableName
	 *            the table name
	 * @return the HTable
	 */
	public static HTable getHTable(String tableName) {
		return (HTable) pool.getTable(tableName);
	}

	/**
	 * Put connection by the given HTable.
	 * 
	 * @param table
	 *            the HTable
	 */
	public static void putHTable(HTable table) {
		if (table != null) {
			pool.putTable(table);
		}
	}

	// Get the class new instance.
	private static <T> T getNewInstance(Class<? extends T> clazz)
			throws HSQLException {
		try {
			return clazz.newInstance();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new HSQLException("Failed to Instance Parser:"
					+ clazz.getName(), e);
		}
	}

	// Execute the HSQL.
	protected static <T> T execute(
			Map<String, Class<? extends HSQLExecutor<T>>> map, String hsql,
			String[] tableNames) throws HSQLException {
		String hsqlFormated = HSQLHelper.formatHSQL(hsql);
		for (String regex : map.keySet()) {
			if (hsqlFormated.matches(regex)) {
				HSQLExecutor<T> executor = getNewInstance(map.get(regex));
				return executor.execute(hsql, tableNames);
			}
		}
		throw new HSQLException("Unsupported HSQL: " + hsql);
	}

	// Execute the HSQL.
	protected static <T> T executeInner(
			Map<String, Class<? extends HSQLInnerExecutor<T>>> map,
			Result result, String mainTableName, String hsql)
			throws HSQLException {
		String hsqlFormated = HSQLHelper.formatHSQL(hsql);
		for (String regex : map.keySet()) {
			if (hsqlFormated.matches(regex)) {
				HSQLInnerExecutor<T> executor = getNewInstance(map.get(regex));
				return executor.execute(result, mainTableName, hsql);
			}
		}
		throw new HSQLException("Unsupported HSQL: " + hsql);
	}
}
