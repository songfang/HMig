package com.tongji.hbase.migration.common;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The table definition.
 * 
 * @author Zhao Long
 */
public class TableDef {

	/**
	 * Supported key type.
	 */
	public enum KeyType {
		INT {
			@Override
			public Class<?> getTypeClass() {
				return Integer.class;
			}
		},
		SMALLINT {
			@Override
			public Class<?> getTypeClass() {
				return Integer.class;
			}
		},
		BIGINT {
			@Override
			public Class<?> getTypeClass() {
				return Integer.class;
			}
		},
		FLOAT {
			@Override
			public Class<?> getTypeClass() {
				return Double.class;
			}
		},
		VARCHAR {
			@Override
			public Class<?> getTypeClass() {
				return String.class;
			}
		},
		NVARCHAR {
			@Override
			public Class<?> getTypeClass() {
				return String.class;
			}
		},
		DATETIME {
			@Override
			public Class<?> getTypeClass() {
				return Date.class;
			}
		},
		TEXT {
			@Override
			public Class<?> getTypeClass() {
				return String.class;
			}
		},
		BIT {
			@Override
			public Class<?> getTypeClass() {
				return Boolean.class;
			}
		};
		public abstract Class<?> getTypeClass();
	}

	private String tableName;
	private Map<String, String> columnKeys;
	private Set<String> primaryKeys;
	private Map<String, String> hasOneRefTables;
	private Map<String, String> hasManyRefTables;

	/**
	 * Constructor.
	 * 
	 * @param tableName
	 *            the table name
	 */
	public TableDef(String tableName) {
		this.tableName = tableName;
		this.columnKeys = new HashMap<String, String>();
		this.primaryKeys = new HashSet<String>();
		this.hasOneRefTables = new HashMap<String, String>();
		this.hasManyRefTables = new HashMap<String, String>();
	}

	/**
	 * Get the primary keys of the table.
	 * 
	 * @return the primary keys
	 */
	public String[] getPrimaryKeys() {
		return this.primaryKeys.toArray(new String[0]);
	}

	/**
	 * Get all keys of the table.
	 * 
	 * @return all keys and their types
	 */
	public Map<String, String> getColumnKeys() {
		return this.columnKeys;
	}

	/**
	 * Get has-one relation tables referred.
	 * 
	 * @return all has-one relation tables referred
	 */
	public Map<String, String> getHasOneRefTables() {
		return hasOneRefTables;
	}

	/**
	 * Get has-many relation tables referred.
	 * 
	 * @return all has-many relation tables referred
	 */
	public Map<String, String> getHasManyRefTables() {
		return hasManyRefTables;
	}

	/**
	 * Get all tables referred.
	 * 
	 * @return all tables referred
	 */
	public Map<String, String> getAllRefTables() {
		Map<String, String> map = new HashMap<String, String>();
		map.putAll(this.hasOneRefTables);
		map.putAll(this.hasManyRefTables);
		return map;
	}

	/**
	 * Get the name of the table.
	 * 
	 * @return the table name
	 */
	public String getTableName() {
		return this.tableName;
	}

	/**
	 * Get the key type by the given key name.
	 * 
	 * @param keyName
	 *            the key name
	 * @return the key type
	 * @throws UnknownKeyException
	 *             when key is unknown
	 * @throws UnsupportedKeyTypeException
	 *             when key type is unknown
	 */
	public KeyType getTypeByKey(String keyName) throws UnknownKeyException,
			UnsupportedKeyTypeException {
		String type = this.columnKeys.get(keyName);
		if (type != null) {
			return getSupportedKeyType(type);
		} else {
			throw new UnknownKeyException(keyName);
		}
	}

	/**
	 * Add the key to primary keys of the table.
	 * 
	 * @param key
	 *            the key name
	 * @throws UnknownKeyException
	 *             when key is unknown
	 */
	public void addPrimaryKey(String key) throws UnknownKeyException {
		if (this.columnKeys.containsKey(key)) {
			this.primaryKeys.add(key);
		} else {
			throw new UnknownKeyException(key);
		}
	}

	/**
	 * Add the key and type pair into the table.
	 * 
	 * @param key
	 *            the key name
	 * @param type
	 *            the type
	 * @throws UnsupportedKeyTypeException
	 *             when type is unsupported
	 */
	public void addKey(String key, String type)
			throws UnsupportedKeyTypeException {
		if (isTypeSupported(type)) {
			this.columnKeys.put(key, type);
		} else {
			throw new UnsupportedKeyTypeException(type);
		}
	}

	/**
	 * Build the relation between two tables.
	 * 
	 * @param tableDef
	 *            the other table definition
	 */
	public void buildRelation(TableDef tableDef) {
		if (this == tableDef) {
			return;
		}

		String hasOneKeyName = tableDef.getTableName() + "ID";
		String hasManyKeyName = tableDef.getTableName() + "IDS";
		for (String keyName : columnKeys.keySet()) {
			String keyNameUpperCase = keyName.toUpperCase();
			if (keyNameUpperCase.equals(hasOneKeyName.toUpperCase())) {
				this.hasOneRefTables.put(tableDef.getTableName(), keyName);
				return;
			} else if (keyNameUpperCase.equals(hasManyKeyName.toUpperCase())) {
				this.hasManyRefTables.put(tableDef.getTableName(), keyName);
				return;
			}
		}
	}

	/**
	 * Build all relations among given tables.
	 * 
	 * @param tableDefs
	 *            all table definitions
	 */
	public static void buildAllRelations(List<TableDef> tableDefs) {
		for (TableDef tableDef : tableDefs) {
			for (TableDef targetDef : tableDefs) {
				tableDef.buildRelation(targetDef);
			}
		}
	}

	/**
	 * Get the supported key type from the given String.
	 * 
	 * @param type
	 *            the type String
	 * @return the supported key type
	 * @throws UnsupportedKeyTypeException
	 *             when the given type is unsupported
	 */
	public static KeyType getSupportedKeyType(String type)
			throws UnsupportedKeyTypeException {
		for (KeyType kt : KeyType.values()) {
			if (type.startsWith(kt.toString())) {
				return kt;
			}
		}
		throw new UnsupportedKeyTypeException(type);
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return tableName.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return tableName.equals(obj);
	}

	@Override
	public String toString() {
		StringBuffer out = new StringBuffer();
		out.append("Name: " + tableName + ", ");
		out.append("Keys: " + columnKeys + ", ");
		out.append("PK: " + primaryKeys + ", ");
		out.append("Has-One: " + hasOneRefTables.size() + ", ");
		out.append("Has-Many: " + hasManyRefTables.size());
		return out.toString();
	}

	// Check if the type is supported.
	private static boolean isTypeSupported(String type) {
		try {
			getSupportedKeyType(type);
			return true;
		} catch (UnsupportedKeyTypeException e) {
			// TODO Auto-generated catch block
			return false;
		}
	}
}
