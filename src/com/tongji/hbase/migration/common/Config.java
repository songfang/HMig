package com.tongji.hbase.migration.common;

import java.io.IOException;

import com.tongji.hbase.migration.util.Logger;
import com.tongji.hbase.migration.util.PropertiesReader;

public class Config implements ConfigDefs {

	private static PropertiesReader properties;

	static {
		try {
			properties = new PropertiesReader(CONFIG_FILE_NAME, Config.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new IllegalArgumentException("Failed to Load File: "
					+ CONFIG_FILE_NAME, e);
		}
	}

	/**
	 * Get the properties.
	 * 
	 * @return the properties object
	 */
	public static PropertiesReader getProperties() {
		return properties;
	}

	/**
	 * Get the logger.
	 * 
	 * @param clazz
	 *            class name to log
	 * @return the logger object
	 */
	public static Logger getLogger(Class<?> clazz) {
		Logger logger = (Logger) getClassInstance(PROPERTY_LOGGER_CLASS);
		logger.init(clazz);
		return logger;
	}

	// Get instance of a class.
	private static Object getClassInstance(String className) {
		try {
			return properties.getClass(PROPERTY_LOGGER_CLASS).newInstance();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new IllegalArgumentException(
					"Failed to Get Instance of Class: " + className, e);
		}
	}
}
