package com.tongji.hbase.migration.convertor;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;

import com.tongji.hbase.migration.DataConvertor;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.common.HTableDef.ColumnInfo;
import com.tongji.hbase.migration.common.TableDef;
import com.tongji.hbase.migration.hsql.HSQLHelper;
import com.tongji.hbase.migration.util.DatabaseUtil;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * Do nest convert.
 * 
 * @author Zhao Long
 */
public class NestConvertor extends DataConvertor {

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
		TableDef ntd = adapter.getTableDefbyName(subTableName);
		ColumnInfo mrc = mhtd.getRowKey();

		ResultSet rs = null;
		List<Object> keyIds = new ArrayList<Object>();
		try {
			rs = this.dbStatement.executeQuery("SELECT " + mrc.getRefKey()
					+ " FROM " + mainTableName);
			while (rs.next()) {
				keyIds.add(getColumnValue(rs, mrc.getRefKey(), mtd));
			}
		} finally {
			DatabaseUtil.close(rs);
		}

		List<Put> puts = new ArrayList<Put>();
		for (Object keyId : keyIds) {
			byte[] rowKey = HSQLHelper.buildRowKey(mhtd, keyId);
			Put put = new Put(rowKey);

			try {
				rs = this.dbStatement.executeQuery("SELECT * FROM "
						+ subTableName + " WHERE "
						+ ntd.getHasOneRefTables().get(mainTableName) + "="
						+ keyId);
				while (rs.next()) {
					Object id = getColumnValue(rs, ntd.getPrimaryKeys()[0], ntd);
					for (ColumnInfo c : mhtd.getColumns(subTableName)) {
						Object val = getColumnValue(rs, c.getRefKey(), ntd);
						HSQLHelper.addSubColumn(put, mhtd, subTableName,
								c.getRefKey(), id, val);
					}
				}
			} finally {
				DatabaseUtil.close(rs);
			}
			if (!put.isEmpty()) {
				puts.add(put);
			}
		}
		HBaseUtil.getHTable(mainTableName).put(puts);
	}
}
