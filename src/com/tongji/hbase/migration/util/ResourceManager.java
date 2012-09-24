package com.tongji.hbase.migration.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Tool to read and write resources.
 * 
 * @author Zhao Long
 */
public class ResourceManager {

	/**
	 * Get resource from current class path.
	 * 
	 * @param resourceName
	 *            the resource name
	 * @param clazz
	 *            the class
	 * @return the input stream
	 */
	public static InputStream getClassResourceAsStream(String resourceName,
			Class<?> clazz) {
		return clazz.getResourceAsStream(resourceName);
	}

	/**
	 * Get resource from jar path.
	 * 
	 * @param resourceName
	 *            the resource name
	 * @param clazz
	 *            the class
	 * @return the input stream
	 * @throws IOException
	 *             when failed to read/create file
	 */
	public static InputStream getJarResourceAsStream(String resourceName,
			Class<?> clazz) throws IOException {
		File f = getJarResourceAsFile(resourceName, clazz);
		return new FileInputStream(f);
	}

	/**
	 * Get file resource from jar path.
	 * 
	 * @param fileName
	 *            the file name
	 * @param clazz
	 *            the class
	 * @return the file
	 * @throws IOException
	 *             when failed to read/create file
	 */
	public static File getJarResourceAsFile(String fileName, Class<?> clazz)
			throws IOException {
		URL url = clazz.getProtectionDomain().getCodeSource().getLocation();
		String filePath = url.getPath();
		File f = new File(filePath);

		if (f.isFile()) {
			filePath = f.getParent();
		}

		f = new File(filePath, fileName);
		f.createNewFile();
		return f;
	}
}
