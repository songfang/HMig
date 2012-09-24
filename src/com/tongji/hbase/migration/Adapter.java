package com.tongji.hbase.migration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.thoughtworks.xstream.XStream;
import com.tongji.hbase.migration.Convertor.RelationType;
import com.tongji.hbase.migration.common.Config;
import com.tongji.hbase.migration.common.ConfigDefs;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.common.HTableDef.ColumnType;
import com.tongji.hbase.migration.common.TableDef;
import com.tongji.hbase.migration.common.TableDef.KeyType;
import com.tongji.hbase.migration.common.UnknownTableException;
import com.tongji.hbase.migration.util.ResourceManager;

/**
 * Tool to adapt TableDef to HTableDef.
 * 
 * @author Zhao Long
 */
public class Adapter implements ConfigDefs {

	/**
	 * Mapping from key type of TableDef to column type HTableDef.
	 */
	public static final EnumMap<KeyType, ColumnType> TYPE_MAPPING;

	static {
		TYPE_MAPPING = new EnumMap<KeyType, ColumnType>(KeyType.class);
		TYPE_MAPPING.put(KeyType.INT, ColumnType.INT);
		TYPE_MAPPING.put(KeyType.SMALLINT, ColumnType.INT);
		TYPE_MAPPING.put(KeyType.BIGINT, ColumnType.INT);
		TYPE_MAPPING.put(KeyType.FLOAT, ColumnType.DOUBLE);
		TYPE_MAPPING.put(KeyType.VARCHAR, ColumnType.STRING);
		TYPE_MAPPING.put(KeyType.NVARCHAR, ColumnType.STRING);
		TYPE_MAPPING.put(KeyType.DATETIME, ColumnType.DATETIME);
		TYPE_MAPPING.put(KeyType.TEXT, ColumnType.STRING);
		TYPE_MAPPING.put(KeyType.BIT, ColumnType.BOOLEAN);
	}

	public static final String GLOBAL_NAME = Config.getProperties()
			.getProperty(PROPERTY_GLOBAL_NAME);
	protected static final String XML_FILE_NAME = GLOBAL_NAME
			+ ".definition.xml";
	protected static Adapter instance = null;

	protected Map<String, TableDef> tableDefs;
	protected Map<String, HTableDef> htableDefs;
	protected EnumMap<RelationType, Map<String, Set<String>>> relationMappings;

	/**
	 * Constructor with empty parameters.
	 */
	public Adapter() {
		this.tableDefs = new HashMap<String, TableDef>();
		this.htableDefs = new HashMap<String, HTableDef>();
		this.relationMappings = new EnumMap<RelationType, Map<String, Set<String>>>(
				RelationType.class);
	}

	/**
	 * Constructor.
	 * 
	 * @param tableDefs
	 *            the table definitions to convert.
	 */
	public Adapter(List<TableDef> tableDefs) {
		this();
		for (TableDef td : tableDefs) {
			this.tableDefs.put(td.getTableName(), td);
		}
	}

	/**
	 * Add relation mappings.
	 * 
	 * @param relationType
	 *            the relation type
	 * @param tableName
	 *            the table name
	 * @param columnFamilyName
	 *            the column family name
	 */
	public void addRelationMapping(RelationType relationType, String tableName,
			String columnFamilyName) {
		Map<String, Set<String>> tableMap = this.relationMappings
				.get(relationType);
		if (tableMap == null) {
			tableMap = new HashMap<String, Set<String>>();
			this.relationMappings.put(relationType, tableMap);
		}
		Set<String> columnFamilySet = tableMap.get(tableName);
		if (columnFamilySet == null) {
			columnFamilySet = new HashSet<String>();
			tableMap.put(tableName, columnFamilySet);
		}
		columnFamilySet.add(columnFamilyName);
	}

	/**
	 * Get all TableDefs.
	 * 
	 * @return all TableDefs
	 */
	public Map<String, TableDef> getTableDefs() {
		return this.tableDefs;
	}

	/**
	 * Get the relation mappings.
	 * 
	 * @return the relation mappings
	 */
	public EnumMap<RelationType, Map<String, Set<String>>> getRelationMappings() {
		return this.relationMappings;
	}

	/**
	 * Get all HTableDefs.
	 * 
	 * @return all HTableDefs
	 */
	public Map<String, HTableDef> getHTableDefs() {
		return this.htableDefs;
	}

	/**
	 * Get TableDef by table name.
	 * 
	 * @param tableName
	 *            the table name
	 * @return the TableDef
	 * @throws UnknownTableException
	 *             when TableDef can't be found by name
	 */
	public TableDef getTableDefbyName(String tableName)
			throws UnknownTableException {
		TableDef td = this.tableDefs.get(tableName);
		if (td == null) {
			throw new UnknownTableException(tableName);
		}
		return td;
	}

	/**
	 * Get HTableDef by table name.
	 * 
	 * @param tableName
	 *            the table name
	 * @return the HTableDef
	 * @throws UnknownTableException
	 *             when HTableDef can't be found by name
	 */
	public HTableDef getHTableDefbyName(String tableName)
			throws UnknownTableException {
		HTableDef htd = this.htableDefs.get(tableName);
		if (htd == null) {
			throw new UnknownTableException(tableName);
		}
		return htd;
	}

	/**
	 * Get HTableDef by TableDef.
	 * 
	 * @param td
	 *            the TableDef
	 * @return the HTableDef
	 * @throws UnknownTableException
	 *             when HTableDef can't be found by TableDef
	 */
	public HTableDef getHTableDefByTableDef(TableDef td)
			throws UnknownTableException {
		return getHTableDefbyName(td.getTableName());
	}

	/**
	 * Serialize the adapter into XML.
	 * 
	 * @throws IOException
	 *             when failed to create XML file
	 */
	public void toXML() throws IOException {
		XStream x = new XStream();
		File f = ResourceManager.getJarResourceAsFile(XML_FILE_NAME,
				Adapter.class);
		FileOutputStream out = new FileOutputStream(f);
		x.toXML(this, out);
	}

	/**
	 * Deserialize the adapter from XML.
	 * 
	 * @return the adapter
	 * @throws IOException
	 *             when failed to write XML file
	 */
	public static Adapter fromXML() throws IOException {
		XStream x = new XStream();
		File f = ResourceManager.getJarResourceAsFile(XML_FILE_NAME,
				Adapter.class);
		return (Adapter) x.fromXML(f);
	}

	/**
	 * Get the singleton instance.
	 * 
	 * @return the adapter
	 */
	public static Adapter getInstance() {
		try {
			if (instance == null) {
				instance = fromXML();
			}
			return instance;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new IllegalArgumentException(
					"Failed to Load Adapter of Name: " + GLOBAL_NAME, e);
		}
	}
}
