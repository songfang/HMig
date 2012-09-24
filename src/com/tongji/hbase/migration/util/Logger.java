package com.tongji.hbase.migration.util;

/**
 * Logger interface to allow different logging providers.
 * 
 * @author Zhao Long
 */
public interface Logger {

	/**
	 * Method allowing to initialize our logger.
	 * 
	 * @param clazz
	 *            class name to log
	 */
	public void init(Class<?> clazz);

	/**
	 * Log message for trace level.
	 * 
	 * @param aMessage
	 *            the message to be logged
	 */
	public void trace(String aMessage);

	/**
	 * Log message for debug level.
	 * 
	 * @param aMessage
	 *            the message to be logged
	 */
	public void debug(String aMessage);

	/**
	 * Log message for info level.
	 * 
	 * @param aMessage
	 *            the message to be logged
	 */
	public void info(String aMessage);

	/**
	 * Log message for warning level.
	 * 
	 * @param aMessage
	 *            the message to be logged
	 */
	public void warn(String aMessage);

	/**
	 * Log message for warning level with exception.
	 * 
	 * @param aMessage
	 *            the message to be logged
	 * @param aThrowable
	 *            the exception
	 */
	public void warn(String aMessage, Throwable aThrowable);

	/**
	 * Log message for error level.
	 * 
	 * @param aMessage
	 *            the message to be logged
	 */
	public void error(String aMessage);

	/**
	 * Log message (error level with exception).
	 * 
	 * @param aMessage
	 *            the message to be logged
	 * @param aThrowable
	 *            the exception
	 */
	public void error(String aMessage, Throwable aThrowable);

	/**
	 * Log message for fatal level.
	 * 
	 * @param aMessage
	 *            the message to be logged
	 */
	public void fatal(String aMessage);

	/**
	 * Log message (fatal level with exception).
	 * 
	 * @param aMessage
	 *            the message to be logged
	 * @param aThrowable
	 *            the exception
	 */
	public void fatal(String aMessage, Throwable aThrowable);

	/**
	 * Set log level.
	 * 
	 * @param aLevel
	 *            a valid Level from ConfigDefs
	 */
	public void setLevel(int aLevel);
}
