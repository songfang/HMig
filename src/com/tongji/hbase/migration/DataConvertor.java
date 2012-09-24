package com.tongji.hbase.migration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import com.tongji.hbase.migration.common.Config;
import com.tongji.hbase.migration.common.TableDef;
import com.tongji.hbase.migration.common.UnknownKeyException;
import com.tongji.hbase.migration.common.UnsupportedKeyTypeException;
import com.tongji.hbase.migration.util.DatabaseUtil;
import com.tongji.hbase.migration.util.Logger;

/**
 * Tool to convert all table data.
 * 
 * @author Zhao Long
 */
public abstract class DataConvertor {

	/**
	 * The id separator.
	 */
	public static final String ID_SEPARATOR = ",";

	protected static final Logger log = Config.getLogger(DataConvertor.class);
	protected static final Adapter adapter = Adapter.getInstance();

	/**
	 * Initialize.
	 * 
	 * @param dbStatement
	 *            the database statement
	 */
	public abstract void init(Statement dbStatement);

	/**
	 * Convert all data.
	 * 
	 * @param mainTableName
	 *            the main table name
	 * @param subTableName
	 *            the sub table name
	 * @throws Exception
	 *             when error occurs
	 */
	public abstract void convert(String mainTableName, String subTableName)
			throws Exception;

	// Get neighbor table between two tables.
	protected static Set<String> getNeighborTables(TableDef mtd, TableDef ttd)
			throws Exception {
		Set<String> set = new HashSet<String>();
		for (String tableName : mtd.getHasOneRefTables().keySet()) {
			TableDef td = adapter.getTableDefbyName(tableName);
			if (td.getHasOneRefTables().keySet().contains(ttd.getTableName())) {
				set.add(tableName);
			}
		}
		return set;
	}

	// Get sub id of the given table name according to TableDef.
	protected static Set<String> getSubIds(TableDef td, String tableName,
			String val) {
		Set<String> set = new HashSet<String>();
		if (td.getHasOneRefTables().keySet().contains(tableName)) {
			set.add(val);
		} else if (td.getHasManyRefTables().keySet().contains(tableName)) {
			String[] subIds = val.split(ID_SEPARATOR);
			for (String subId : subIds) {
				set.add(subId);
			}
		}
		return set;
	}

	// Get value of the key.
	protected static Object getColumnValue(ResultSet rs, String keyName,
			TableDef td) throws UnknownKeyException,
			UnsupportedKeyTypeException, SQLException {
		Class<?> clazz = td.getTypeByKey(keyName).getTypeClass();
		return DatabaseUtil.getValueByType(rs, clazz, keyName);
	}
}
