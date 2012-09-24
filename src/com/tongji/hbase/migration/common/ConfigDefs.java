package com.tongji.hbase.migration.common;

/**
 * Definition of configuration property strings.
 * 
 * @author Zhao Long
 */
public interface ConfigDefs {

	public static final String CONFIG_FILE_NAME = "config.properties";

	public static final String PROPERTY_GLOBAL_NAME = "global.name";

	/**
	 * Database configurations.
	 */
	public static final String PROPERTY_DATABASE_DRIVER = "database.driver";
	public static final String PROPERTY_DATABASE_URL = "database.url";
	public static final String PROPERTY_DATABASE_USERNAME = "database.username";
	public static final String PROPERTY_DATABASE_PASSWORD = "database.password";

	/**
	 * HBase configurations.
	 */
	public static final String PROPERTY_HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String PROPERTY_HBASE_POOL_SIZE = "hbase.pool.size";
	
	/**
	 * Class factory definitions, used to insert custom classes.
	 */
	public static final String PROPERTY_LOGGER_CLASS = "logger.class";
	public static final String PROPERTY_HSQL_EXECUTOR_CLASS = "hsql.executor.class";

}
