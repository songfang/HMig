package com.tongji.hbase.migration.convertor;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Put;

import com.tongji.hbase.migration.DataConvertor;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.common.HTableDef.ColumnInfo;
import com.tongji.hbase.migration.common.TableDef;
import com.tongji.hbase.migration.hsql.HSQLHelper;
import com.tongji.hbase.migration.util.DatabaseUtil;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * Do split convert.
 * 
 * @author Zhao Long
 */
public class SplitConvertor extends DataConvertor {

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
		TableDef std = adapter.getTableDefbyName(subTableName);
		ColumnInfo mrc = mhtd.getRowKey();
		String mk = mrc.getRefKey();
		String sk = mtd.getAllRefTables().get(subTableName);

		ResultSet rs = null;
		Map<Object, String> map = new HashMap<Object, String>();
		try {
			rs = this.dbStatement.executeQuery("SELECT " + mk + "," + sk
					+ " FROM " + mainTableName);
			while (rs.next()) {
				map.put(getColumnValue(rs, mk, mtd),
						getColumnValue(rs, sk, mtd).toString());
			}
		} finally {
			DatabaseUtil.close(rs);
		}

		List<Put> puts = new ArrayList<Put>();
		for (Entry<Object, String> e : map.entrySet()) {
			Object keyId = e.getKey();
			byte[] rowKey = HSQLHelper.buildRowKey(mhtd, keyId);
			Put put = new Put(rowKey);

			for (String valId : getSubIds(mtd, subTableName, e.getValue())) {

				try {
					rs = this.dbStatement.executeQuery("SELECT * FROM "
							+ subTableName + " WHERE "
							+ std.getPrimaryKeys()[0] + "=" + valId);

					while (rs.next()) {
						Object id = getColumnValue(rs, std.getPrimaryKeys()[0],
								std);
						for (ColumnInfo c : mhtd.getColumns(subTableName)) {
							Object val = getColumnValue(rs, c.getRefKey(), std);
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
