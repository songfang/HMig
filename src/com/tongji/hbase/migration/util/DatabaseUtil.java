package com.tongji.hbase.migration.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;

import com.tongji.hbase.migration.common.Config;
import com.tongji.hbase.migration.common.ConfigDefs;

/**
 * Tool to operate database.
 * 
 * @author Zhao Long
 */
public class DatabaseUtil implements ConfigDefs {

	private static final Logger log = Config.getLogger(DatabaseUtil.class);

	private static final String driver = Config.getProperties().getProperty(
			PROPERTY_DATABASE_DRIVER);
	private static final String url = Config.getProperties().getProperty(
			PROPERTY_DATABASE_URL);
	private static final String username = Config.getProperties().getProperty(
			PROPERTY_DATABASE_USERNAME);
	private static final String password = Config.getProperties().getProperty(
			PROPERTY_DATABASE_PASSWORD);

	/**
	 * Get database connection.
	 * 
	 * @return the database connection
	 * @throws Exception
	 *             when failed to get database connection
	 */
	public static Connection getConnection() throws Exception {
		Class.forName(driver);
		return DriverManager.getConnection(url, username, password);
	}

	/**
	 * Close the database connection.
	 * 
	 * @param connection
	 *            the database connection
	 */
	public static void close(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.warn("Failed to Close Database Connection", e);
			}
		}
		connection = null;
	}

	/**
	 * Close the database statement.
	 * 
	 * @param statement
	 *            the database statement
	 */
	public static void close(Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.warn("Failed to Close Database Statement", e);
			}
		}
		statement = null;
	}

	/**
	 * Close the database result set.
	 * 
	 * @param rs
	 *            the database result set
	 */
	public static void close(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.warn("Failed to Close Database Result Set", e);
			}
		}
		rs = null;
	}

	/**
	 * Get the value in ResultSet.
	 * 
	 * @param rs
	 *            the ResultSet
	 * @param clazz
	 *            the key type class
	 * @param keyName
	 *            the key name
	 * @return the value
	 * @throws SQLException
	 *             when key type class is unsupported
	 */
	public static Object getValueByType(ResultSet rs, Class<?> clazz,
			String keyName) throws SQLException {
		if (String.class.equals(clazz)) {
			return rs.getString(keyName);
		} else if (Integer.class.equals(clazz)) {
			return rs.getInt(keyName);
		} else if (Double.class.equals(clazz)) {
			return rs.getDouble(keyName);
		} else if (Boolean.class.equals(clazz)) {
			return rs.getBoolean(keyName);
		} else if (Date.class.equals(clazz)) {
			Timestamp ts = rs.getTimestamp(keyName);
			return new Date(ts.getTime());
		} else {
			throw new SQLException("Unsupported Type to Get from ResultSet: "
					+ clazz.getSimpleName());
		}
	}
}
