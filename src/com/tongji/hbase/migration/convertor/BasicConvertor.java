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
 * Do basic convert.
 * 
 * @author Zhao Long
 */
public class BasicConvertor extends DataConvertor {

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
		HTableDef htd = adapter.getHTableDefbyName(mainTableName);
		TableDef td = adapter.getTableDefbyName(mainTableName);
		ColumnInfo rc = htd.getRowKey();

		ResultSet rs = null;
		List<Put> puts = new ArrayList<Put>();
		try {
			rs = this.dbStatement
					.executeQuery("SELECT * FROM " + mainTableName);
			while (rs.next()) {
				Object keyId = getColumnValue(rs, rc.getRefKey(), td);

				byte[] rowKey = HSQLHelper.buildRowKey(htd, keyId);
				Put put = new Put(rowKey);
				for (ColumnInfo c : htd.getColumns(mainTableName)) {
					Object val = getColumnValue(rs, c.getRefKey(), td);
					HSQLHelper.addColumn(put, htd, mainTableName,
							c.getRefKey(), val);
				}
				if (!put.isEmpty()) {
					puts.add(put);
				}
			}
		} finally {
			DatabaseUtil.close(rs);
		}
		HBaseUtil.getHTable(mainTableName).put(puts);
	}
}
