package com.tongji.hbase.migration.convertor;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;

import com.tongji.hbase.migration.DataConvertor;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.common.HTableDef.ColumnInfo;
import com.tongji.hbase.migration.common.TableDef;
import com.tongji.hbase.migration.hsql.HSQLHelper;
import com.tongji.hbase.migration.util.DatabaseUtil;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * Do in-line convert.
 * 
 * @author Zhao Long
 */
public class InlineConvertor extends DataConvertor {

	private Statement dbStatement;

	@Override
	public void init(Statement dbStatement) {
		// TODO Auto-generated method stub
		this.dbStatement = dbStatement;
	}

	@Override
	public void convert(String mainTableName, String subTableName)
			throws Exception {
		// TODO Auto-generated method stub
		HTableDef mhtd = adapter.getHTableDefbyName(mainTableName);
		TableDef mtd = adapter.getTableDefbyName(mainTableName);
		TableDef itd = adapter.getTableDefbyName(subTableName);
		ColumnInfo mrc = mhtd.getRowKey();
		String mk = mrc.getRefKey();
		String ik = itd.getPrimaryKeys()[0];

		ResultSet rs = null;
		Set<Object> set = new HashSet<Object>();
		try {
			rs = this.dbStatement.executeQuery("SELECT " + mk + " FROM "
					+ mainTableName);
			while (rs.next()) {
				set.add(getColumnValue(rs, mk, mtd));
			}
		} finally {
			DatabaseUtil.close(rs);
		}

		Set<String> neighborSet = getNeighborTables(itd, mtd);
		if (neighborSet.size() == 0) {
			log.warn("No Data Found between Tables: " + mtd.getTableName()
					+ " , " + itd.getTableName());
			return;
		}

		Map<Object, Set<Object>> map = new HashMap<Object, Set<Object>>();
		for (Object keyId : set) {
			Set<Object> inlineIdSet = new HashSet<Object>();
			for (String neighborTableName : neighborSet) {
				TableDef td = adapter.getTableDefbyName(neighborTableName);
				String refKeyName = itd.getHasOneRefTables().get(
						neighborTableName);
				try {
					rs = this.dbStatement.executeQuery("SELECT " + subTableName
							+ "." + itd.getPrimaryKeys()[0] + " FROM "
							+ subTableName + "," + neighborTableName
							+ " WHERE " + subTableName + "." + refKeyName + "="
							+ neighborTableName + "." + refKeyName + " AND "
							+ neighborTableName + "."
							+ td.getHasOneRefTables().get(mainTableName) + "="
							+ keyId);

					while (rs.next()) {
						inlineIdSet.add(getColumnValue(rs, ik, itd));
					}
				} finally {
					DatabaseUtil.close(rs);
				}
			}
			if (inlineIdSet.size() > 0) {
				map.put(keyId, inlineIdSet);
			}
		}

		List<Put> puts = new ArrayList<Put>();
		for (Entry<Object, Set<Object>> e : map.entrySet()) {
			Object keyId = e.getKey();
			byte[] rowKey = HSQLHelper.buildRowKey(mhtd, keyId);
			Put put = new Put(rowKey);

			for (Object inlineId : e.getValue()) {
				try {
					rs = this.dbStatement.executeQuery("SELECT * FROM "
							+ subTableName + " WHERE " + ik + "=" + inlineId);
					while (rs.next()) {
						Object id = getColumnValue(rs, itd.getPrimaryKeys()[0],
								itd);
						for (ColumnInfo c : mhtd.getColumns(subTableName)) {
							Object val = getColumnValue(rs, c.getRefKey(), itd);
							HSQLHelper.addSubColumn(put, mhtd, subTableName,
									c.getRefKey(), id, val);
						}
					}
				} finally {
					DatabaseUtil.close(rs);
				}
			}
			if (!put.isEmpty()) {
				puts.add(put);
			}
		}
		HBaseUtil.getHTable(mainTableName).put(puts);
	}
}
