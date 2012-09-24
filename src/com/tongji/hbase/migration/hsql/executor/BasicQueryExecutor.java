package com.tongji.hbase.migration.hsql.executor;

import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.tongji.hbase.migration.common.HSQLException;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.common.UnknownTableException;
import com.tongji.hbase.migration.hsql.HSQLConnection;
import com.tongji.hbase.migration.hsql.HSQLExecutor;
import com.tongji.hbase.migration.hsql.HSQLExecutorBasic;
import com.tongji.hbase.migration.hsql.HSQLParser;
import com.tongji.hbase.migration.hsql.HSQLScanner;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * Basic query executor.
 * 
 * @author Zhao Long
 */
public class BasicQueryExecutor extends HSQLExecutorBasic implements
		HSQLExecutor<HSQLScanner> {

	static {
		HSQLConnection.registerQueryExecutor(HSQL_WORD_SELECT + " (\\*|([\\w"
				+ HSQL_SYM_COMMA + "]+)) " + HSQL_WORD_FROM
				+ " \\w+( WHERE ((\\w+=[\\d\\.]+)| AND |(\\w+='[^']*'))+)?",
				BasicQueryExecutor.class);
	}

	@Override
	public HSQLScanner execute(String hsql, String[] tableNames)
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

		Scan scan = new Scan();
		if (itemNames.length == 0) {
			scan = HBaseUtil.addScanFamily(scan, tableName);
		} else {
			for (String itemName : itemNames) {
				scan = HBaseUtil.addScanColumn(scan, tableName, itemName);
			}
		}

		for (String name : tableNames) {
			if (!name.equals(tableName)) {
				scan = HBaseUtil.addScanFamily(scan, name);
			}
		}

		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
		for (Entry<String, Object> e : parser.getWherePairs().entrySet()) {
			String columName = e.getKey();
			Class<?> clazz = htd.getColumn(tableName, columName)
					.getColumnType().getTypeClass();

			SingleColumnValueFilter filter = new SingleColumnValueFilter(
					Bytes.toBytes(tableName), Bytes.toBytes(columName),
					CompareOp.EQUAL, toBytes(clazz, e.getValue()));
			filter.setFilterIfMissing(true);
			filterList.addFilter(filter);
		}
		scan.setFilter(filterList);

		HTable table = HSQLConnection.getHTable(tableName);
		ResultScanner scanner = getScanner(table, scan);
		return new BasicScanner(table, tableName, scanner);
	}
}
