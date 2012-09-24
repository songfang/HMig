package com.tongji.hbase.migration.hsql.executor;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import com.tongji.hbase.migration.common.HSQLException;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.common.UnknownTableException;
import com.tongji.hbase.migration.hsql.HSQLConnection;
import com.tongji.hbase.migration.hsql.HSQLExecutor;
import com.tongji.hbase.migration.hsql.HSQLExecutorBasic;
import com.tongji.hbase.migration.hsql.HSQLHelper;
import com.tongji.hbase.migration.hsql.HSQLParser;
import com.tongji.hbase.migration.hsql.HSQLResult;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * Basic get executor.
 * 
 * @author Zhao Long
 */
public class BasicGetExecutor extends HSQLExecutorBasic implements
		HSQLExecutor<HSQLResult> {

	static {
		HSQLConnection.registerGetExecutor(HSQL_WORD_SELECT + " (\\*|([\\w"
				+ HSQL_SYM_COMMA + "]+)) " + HSQL_WORD_FROM + " \\w+ "
				+ HSQL_WORD_WHERE + " \\w+=\\d+", BasicGetExecutor.class);
	}

	@Override
	public HSQLResult execute(String hsql, String[] tableNames)
			throws HSQLException {
		// TODO Auto-generated method stub
		HSQLParser parser = new HSQLParser(hsql);
		String tableName = parser.getTableName();
		String[] itemNames = parser.getSelectItems();

		HTableDef htd = null;
		try {
			htd = adapter.getHTableDefbyName(tableName);
		} catch (UnknownTableException e) {
			// TODO Auto-generated catch block
			throw new HSQLException("Failed to Get the HTableDef of: "
					+ tableName, e);
		}

		Get get = new Get(HSQLHelper.buildRowKey(htd, parser.getWherePairs()
				.get(htd.getRowKey().getColumnName())));

		if (itemNames.length == 0) {
			get = HBaseUtil.addGetFamily(get, tableName);
		} else {
			for (String itemName : itemNames) {
				get = HBaseUtil.addGetColumn(get, tableName, itemName);
			}
		}

		for (String name : tableNames) {
			if (!name.equals(tableName)) {
				get = HBaseUtil.addGetFamily(get, name);
			}
		}

		HTable table = null;
		try {
			table = HSQLConnection.getHTable(tableName);
			Result result = getResult(table, get);
			return new HSQLResult(tableName, result);
		} finally {
			HSQLConnection.putHTable(table);
		}
	}
}
