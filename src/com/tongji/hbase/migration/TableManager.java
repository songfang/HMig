package com.tongji.hbase.migration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.EnumMap;
import java.util.Set;

import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.tongji.hbase.migration.Convertor.RelationType;
import com.tongji.hbase.migration.common.Config;
import com.tongji.hbase.migration.common.ConfigDefs;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.convertor.BasicConvertor;
import com.tongji.hbase.migration.convertor.InlineConvertor;
import com.tongji.hbase.migration.convertor.NestConvertor;
import com.tongji.hbase.migration.convertor.SplitConvertor;
import com.tongji.hbase.migration.util.DatabaseUtil;
import com.tongji.hbase.migration.util.HBaseUtil;
import com.tongji.hbase.migration.util.Logger;

/**
 * Tool to manage tables.
 * 
 * @author Zhao Long
 */
public class TableManager implements ConfigDefs {

	private static final Logger log = Config.getLogger(TableManager.class);
	protected static final Adapter adapter = Adapter.getInstance();
	protected static final EnumMap<RelationType, DataConvertor> convertors;

	static {
		convertors = new EnumMap<RelationType, DataConvertor>(
				RelationType.class);
		convertors.put(RelationType.NEST, new NestConvertor());
		convertors.put(RelationType.SPLIT, new SplitConvertor());
		convertors.put(RelationType.INLINE, new InlineConvertor());
	}

	protected Connection dbConnection;
	protected Statement dbStatement;
	protected HBaseAdmin hbAdmin;

	/**
	 * Constructor.
	 * 
	 * @throws Exception
	 *             when failed to create connections
	 */
	public TableManager() throws Exception {
		this.dbConnection = DatabaseUtil.getConnection();
		this.dbStatement = this.dbConnection.createStatement();
		this.hbAdmin = HBaseUtil.getAdmin();
	}

	/**
	 * Delete all tables in HBase.
	 */
	public void deleteAllHTables() {
		for (String tableName : adapter.getHTableDefs().keySet()) {
			try {
				if (hbAdmin.tableExists(tableName)) {
					HBaseUtil.deleteHTable(this.hbAdmin, tableName);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.warn("Failed to Delete HTable: " + tableName, e);
			}
		}
	}

	/**
	 * Recreate all tables in HBase.
	 */
	public void recreateAllHTables() {
		for (String tableName : adapter.getHTableDefs().keySet()) {
			recreateHTable(tableName);
		}
	}

	/**
	 * Recreate the table by the given table name.
	 * 
	 * @param tableName
	 *            the table name.
	 */
	public void recreateHTable(String tableName) {
		try {
			HTableDef htd = adapter.getHTableDefbyName(tableName);
			if (hbAdmin.tableExists(tableName)) {
				HBaseUtil.deleteHTable(this.hbAdmin, tableName);
			}
			HBaseUtil.creatHTable(this.hbAdmin, tableName,
					htd.getColumnFamilies());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.warn("Failed to Create HTable: " + tableName, e);
		}
	}

	/**
	 * Do the clean.
	 */
	public void destroy() {
		DatabaseUtil.close(this.dbStatement);
		DatabaseUtil.close(this.dbConnection);
	}

	/**
	 * Migrate data in the given table.
	 * 
	 * @param tableName
	 *            the table name
	 */
	public void migrateHTable(String tableName) {
		Set<String> set = null;
		DataConvertor convertor = null;
		try {
			convertor = new BasicConvertor();
			convertor.init(this.dbStatement);
			convertor.convert(tableName, null);

			for (RelationType type : RelationType.values()) {
				set = adapter.getRelationMappings().get(type).get(tableName);
				if (set != null) {
					for (String subTableName : set) {
						convertor = convertors.get(type);
						convertor.init(this.dbStatement);
						convertor.convert(tableName, subTableName);
					}
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			log.warn("Failed to Migrate Data in Table: " + tableName, e);
		}
	}

	/**
	 * Migrate data in all table.
	 */
	public void migrateAllHTables() {
		for (String tableName : adapter.getHTableDefs().keySet()) {
			migrateHTable(tableName);
		}
	}

	public static void main(String[] args) throws Exception {
		TableManager tm = new TableManager();
		//tm.recreateHTable("Cantavil");
		//tm.migrateHTable("Cantavil");
		tm.recreateAllHTables();
		tm.migrateAllHTables();
		tm.destroy();
	}
}
