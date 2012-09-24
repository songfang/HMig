package com.tongji.hbase.migration.common;

import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * The table definition of HBase.
 * 
 * @author Zhao Long
 */
public class HTableDef {

	/**
	 * Supported column type.
	 */
	public enum ColumnType {
		INT {
			@Override
			public Class<?> getTypeClass() {
				return Integer.class;
			}
		},
		DOUBLE {
			@Override
			public Class<?> getTypeClass() {
				return Double.class;
			}
		},
		STRING {
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
		BOOLEAN {
			@Override
			public Class<?> getTypeClass() {
				return Boolean.class;
			}
		};
		public abstract Class<?> getTypeClass();
	}

	private String tableName;
	private String rowKey;
	private Map<String, Map<String, ColumnInfo>> columnInfos;

	/**
	 * Constructor.
	 * 
	 * @param tableName
	 *            the table name
	 */
	public HTableDef(String tableName) {
		this.tableName = tableName;
		this.rowKey = null;
		this.columnInfos = new HashMap<String, Map<String, ColumnInfo>>();
	}

	/**
	 * Add column into the table definition.
	 * 
	 * @param columnFamilyName
	 *            the column family name
	 * @param columnName
	 *            the column name
	 * @param columnType
	 *            the column type
	 * @param refTable
	 *            the table referred
	 * @param refKey
	 *            the key referred
	 * @throws UnsupportedKeyTypeException
	 *             when the column type is unsupported
	 */
	public void addColumn(String columnFamilyName, String columnName,
			ColumnType columnType, TableDef refTable, String refKey)
			throws UnsupportedKeyTypeException {
		Map<String, ColumnInfo> map = this.columnInfos.get(columnFamilyName);
		if (map == null) {
			map = new HashMap<String, ColumnInfo>();
			this.columnInfos.put(columnFamilyName, map);
		}
		map.put(columnName, new ColumnInfo(columnName, columnType, refTable,
				refKey));
	}

	/**
	 * Set the row key of the table.
	 * 
	 * @param columnName
	 *            the row key name
	 * @throws UnknownColumnException
	 *             when key is unknown
	 */
	public void setRowKey(String columnName) throws UnknownColumnException {
		Map<String, ColumnInfo> map = this.columnInfos.get(tableName);
		if (map.keySet().contains(columnName)) {
			this.rowKey = columnName;
		} else {
			throw new UnknownColumnException(columnName);
		}
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
	 * Get the row key of the table.
	 * 
	 * @return the row key
	 */
	public ColumnInfo getRowKey() {
		Map<String, ColumnInfo> map = this.columnInfos.get(tableName);
		return map.get(this.rowKey);
	}

	/**
	 * Get all columns by the column family name.
	 * 
	 * @param columnFamilyName
	 *            the column family name
	 * @return all columns
	 */
	public Collection<ColumnInfo> getColumns(String columnFamilyName) {
		return this.columnInfos.get(columnFamilyName).values();
	}

	/**
	 * Get the column.
	 * 
	 * @param columnFamilyName
	 *            the column family name
	 * @param columnName
	 *            the column name
	 * @return the column
	 */
	public ColumnInfo getColumn(String columnFamilyName, String columnName) {
		return this.columnInfos.get(columnFamilyName).get(columnName);
	}

	/**
	 * Get all column families.
	 * 
	 * @return all column families
	 */
	public Set<String> getColumnFamilies() {
		return this.columnInfos.keySet();
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
		out.append("RowKey: " + rowKey + ", ");
		out.append("Columns: " + columnInfos);
		return out.toString();
	}

	/**
	 * Class to carry basic information of the column.
	 * 
	 * @author Zhao Long
	 */
	public class ColumnInfo {

		private String columnName;
		private ColumnType columnType;
		private String refTable;
		private String refKey;

		/**
		 * Constructor.
		 * 
		 * @param columnName
		 *            the column name
		 * @param columnType
		 *            the column type
		 * @param refTable
		 *            the table referred
		 * @param refKey
		 *            the key referred
		 * @throws UnsupportedKeyTypeException
		 *             when the column type is unsupported
		 */
		public ColumnInfo(String columnName, ColumnType columnType,
				TableDef refTable, String refKey)
				throws UnsupportedKeyTypeException {
			if (!EnumSet.allOf(ColumnType.class).contains(columnType)) {
				throw new UnsupportedKeyTypeException(columnType.toString());
			}

			this.columnName = columnName;
			this.columnType = columnType;
			this.refTable = refTable.getTableName();
			this.refKey = refKey;
		}

		/**
		 * Get name of the column.
		 * 
		 * @return column name
		 */
		public String getColumnName() {
			return columnName;
		}

		/**
		 * Get type of the column.
		 * 
		 * @return column type
		 */
		public ColumnType getColumnType() {
			return columnType;
		}

		/**
		 * Get table the column referred.
		 * 
		 * @return table definition
		 */
		public String getRefTable() {
			return refTable;
		}

		/**
		 * Get key the column referred.
		 * 
		 * @return key name
		 */
		public String getRefKey() {
			return refKey;
		}

		@Override
		public int hashCode() {
			// TODO Auto-generated method stub
			return columnName.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			// TODO Auto-generated method stub
			return columnName.equals(obj);
		}

		@Override
		public String toString() {
			StringBuffer out = new StringBuffer();
			out.append("(");
			out.append(columnName + ", ");
			out.append(columnType + ", ");
			out.append("Refer:" + refTable + "--" + refKey);
			out.append(")");
			return out.toString();
		}
	}

}
