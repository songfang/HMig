package com.tongji.hbase.migration;

import java.util.List;
import java.util.Map.Entry;

import com.tongji.hbase.migration.common.Config;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.common.HTableDef.ColumnType;
import com.tongji.hbase.migration.common.TableDef;
import com.tongji.hbase.migration.common.TableDef.KeyType;
import com.tongji.hbase.migration.common.UnknownColumnException;
import com.tongji.hbase.migration.common.UnknownTableException;
import com.tongji.hbase.migration.common.UnsupportedKeyTypeException;
import com.tongji.hbase.migration.util.Logger;

/**
 * Tool to convert table definition.
 * 
 * @author Zhao Long
 */
public class Convertor {

	/**
	 * Relation types.
	 */
	public enum RelationType {
		NEST, INLINE, SPLIT
	}

	private static final Logger log = Config.getLogger(Convertor.class);

	protected Adapter adapter;

	/**
	 * Constructor.
	 * 
	 * @param tableDefs
	 *            the table definitions to convert.
	 */
	public Convertor(List<TableDef> tableDefs) {
		adapter = new Adapter(tableDefs);
	}

	/**
	 * Get the mapped column type from the given key type.
	 * 
	 * @param type
	 *            the key type
	 * @return the column type
	 * @throws UnsupportedKeyTypeException
	 *             when the key type has no mapping
	 */
	public static ColumnType getMappedColumnType(String type)
			throws UnsupportedKeyTypeException {
		KeyType keyType = TableDef.getSupportedKeyType(type);
		ColumnType columnType = Adapter.TYPE_MAPPING.get(keyType);
		if (columnType == null) {
			throw new UnsupportedKeyTypeException(type);
		}
		return columnType;
	}

	/**
	 * Do one-to-one basic convert.
	 * 
	 * @throws UnsupportedKeyTypeException
	 *             when meet unsupported key type
	 * @throws UnknownColumnException
	 *             when meet unknown column
	 */
	public void basic() throws UnsupportedKeyTypeException,
			UnknownColumnException {
		for (TableDef td : adapter.getTableDefs().values()) {
			if (td.getPrimaryKeys().length != 1) {
				throw new IllegalArgumentException(
						"Only Single Primary Key is Supported");
			}
			HTableDef htd = new HTableDef(td.getTableName());
			migrateDef(td, htd);
			htd.setRowKey(td.getPrimaryKeys()[0]);
			adapter.getHTableDefs().put(htd.getTableName(), htd);
		}
	}

	/**
	 * Do nest convert.
	 * 
	 * @param tableName
	 *            the table to do convert
	 * @param toDrop
	 *            if to drop the converted table
	 * @throws UnknownTableException
	 *             when table name is unknown
	 * @throws UnsupportedKeyTypeException
	 *             when meet unsupported key type
	 */
	public void nest(String tableName, boolean toDrop)
			throws UnsupportedKeyTypeException, UnknownTableException {
		try {
			TableDef ntd = this.adapter.getTableDefbyName(tableName);

			if (ntd.getHasOneRefTables().keySet().size() == 0) {
				log.error("No Target Table to Do Nest Convert for Table: " + tableName);
				throw new UnknownTableException(tableName);
			}

			for (String name : ntd.getHasOneRefTables().keySet()) {
				try {
					migrateDef(ntd, this.adapter.getHTableDefbyName(name));
					this.adapter.addRelationMapping(RelationType.NEST, name,
							tableName);
				} catch (UnknownTableException e) {
					// TODO Auto-generated catch block
					log.warn("Missing HTable Definition", e);
				}
			}
			if (toDrop) {
				adapter.getHTableDefs().remove(ntd.getTableName());
			}
			log.info("Succeed to Do Nest Convert to Table: " + tableName);
		} catch (UnknownTableException ute) {
			log.error("Failed to Do Nest Convert to Table: " + tableName, ute);
			throw ute;
		} catch (UnsupportedKeyTypeException ukte) {
			log.error("Failed to Do Nest Convert to Table: " + tableName, ukte);
			throw ukte;
		}
	}

	/**
	 * Do in-line convert.
	 * 
	 * @param srcTableName
	 *            the table to be built in-line
	 * @param dstTableName
	 *            the table to build the in-line table to
	 * @param toDrop
	 *            if to drop the converted table
	 * @throws UnknownTableException
	 *             when table name is unknown
	 * @throws UnsupportedKeyTypeException
	 *             when meet unsupported key type
	 */
	public void inline(String srcTableName, String dstTableName, boolean toDrop)
			throws UnknownTableException, UnsupportedKeyTypeException {
		try {
			TableDef td = this.adapter.getTableDefbyName(srcTableName);
			HTableDef htd = this.adapter.getHTableDefbyName(dstTableName);
			migrateDef(td, htd);
			this.adapter.addRelationMapping(RelationType.INLINE, dstTableName,
					srcTableName);
			if (toDrop) {
				this.adapter.getHTableDefs().remove(srcTableName);
			}
			log.info("Succeed to Do Inline Convert to Table: " + srcTableName);
		} catch (UnknownTableException ute) {
			log.error("Failed to Do Inline Convert to Table: " + srcTableName,
					ute);
			throw ute;
		} catch (UnsupportedKeyTypeException ukte) {
			log.error("Failed to Do Inline Convert to Table: " + srcTableName,
					ukte);
			throw ukte;
		}
	}

	/**
	 * Do split convert.
	 * 
	 * @param tableName
	 *            the table to do convert
	 * @param toDrop
	 *            if to drop the converted table
	 * @throws UnknownTableException
	 *             when table name is unknown
	 * @throws UnsupportedKeyTypeException
	 *             when meet unsupported key type
	 */
	public void split(String tableName, boolean toDrop)
			throws UnknownTableException, UnsupportedKeyTypeException {
		try {
			TableDef ntd = this.adapter.getTableDefbyName(tableName);
			for (TableDef td : this.adapter.getTableDefs().values()) {
				if (td.getAllRefTables().keySet().contains(ntd)) {
					try {
						migrateDef(ntd, this.adapter.getHTableDefByTableDef(td));
						this.adapter.addRelationMapping(RelationType.SPLIT,
								td.getTableName(), tableName);
					} catch (UnknownTableException e) {
						// TODO Auto-generated catch block
						log.warn("Missing HTable Definition", e);
					}
				}
			}
			if (toDrop) {
				adapter.getHTableDefs().remove(ntd.getTableName());
			}
			log.info("Succeed to Do Split Convert to Table: " + tableName);
		} catch (UnknownTableException ute) {
			log.error("Failed to Do Split Convert to Table: " + tableName, ute);
			throw ute;
		} catch (UnsupportedKeyTypeException ukte) {
			log.error("Failed to Do Split Convert to Table: " + tableName, ukte);
			throw ukte;
		}
	}

	/**
	 * Get the adapter.
	 * 
	 * @return the adapter.
	 */
	public Adapter getAdapter() {
		return this.adapter;
	}

	// Migrate definition from TableDef to HTableDef.
	private void migrateDef(TableDef td, HTableDef htd)
			throws UnsupportedKeyTypeException {
		for (Entry<String, String> e : td.getColumnKeys().entrySet()) {
			htd.addColumn(td.getTableName(), e.getKey(),
					getMappedColumnType(e.getValue()), td, e.getKey());
		}
	}
}
