package com.tongji.hbase.migration.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Tool to load property files.
 * 
 * @author Zhao Long
 */
public class PropertiesReader {

	public static final String CLASS_SEPARATOR = ";";

	protected Properties properties;

	/**
	 * Load the properties file by file name.
	 * 
	 * @param fileName
	 *            file to load from
	 * @throws IOException
	 *             when failed to load file
	 */
	public PropertiesReader(String fileName, Class<?> clazz) throws IOException {
		properties = new Properties();

		// Load from current class path as default vales.
		properties.load(ResourceManager.getClassResourceAsStream(fileName,
				clazz));

		// Load from root class path.
		properties = new Properties(properties);
		properties
				.load(ResourceManager.getJarResourceAsStream(fileName, clazz));
	}

	/**
	 * Get a certain property from a name.
	 * 
	 * @param aName
	 *            the name of a property
	 * @return the value
	 */
	public String getProperty(String aName) {
		String property = properties.getProperty(aName);
		if (property == null) {
			throw new IllegalArgumentException("Unknown Property: " + aName);
		}
		return property;
	}

	/**
	 * Get a property as boolean.
	 * 
	 * @param aName
	 *            the name of a property
	 * @return the value
	 */
	public boolean getBoolProperty(String aName) {
		return Boolean.parseBoolean(getProperty(aName));
	}

	/**
	 * Get a property as int.
	 * 
	 * @param aName
	 *            the name of a property
	 * @return the value
	 */
	public int getIntProperty(String aName) {
		return Integer.parseInt(getProperty(aName));
	}

	/**
	 * Get a property as long.
	 * 
	 * @param aName
	 *            the name of a property
	 * @return the value
	 */
	public long getLongProperty(String aName) {
		return Long.parseLong(getProperty(aName));
	}

	/**
	 * Get a property as Classes.
	 * 
	 * @param aClassNameProp
	 *            the name of a property
	 * @return the values
	 */
	public Class<?>[] getAllClasses(String aClassNameProp) {
		try {
			String[] args = getProperty(aClassNameProp).split(CLASS_SEPARATOR);
			List<Class<?>> list = new ArrayList<Class<?>>();
			for (String className : args) {
				Class<?> clazz = Class.forName(className.trim());
				list.add(clazz);
			}
			return list.toArray(new Class<?>[] {});
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			throw new IllegalArgumentException("Unknown Class: "
					+ aClassNameProp, e);
		}
	}

	/**
	 * Get a property as a Class.
	 * 
	 * @param aClassNameProp
	 *            the name of a property
	 * @return the value
	 */
	public Class<?> getClass(String aClassNameProp) {
		return getAllClasses(aClassNameProp)[0];
	}
}
