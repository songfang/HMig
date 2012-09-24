package com.tongji.hbase.migration.hsql.executor;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.tongji.hbase.migration.common.HSQLException;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.common.UnknownTableException;
import com.tongji.hbase.migration.hsql.HSQLConnection;
import com.tongji.hbase.migration.hsql.HSQLExecutorBasic;
import com.tongji.hbase.migration.hsql.HSQLHelper;
import com.tongji.hbase.migration.hsql.HSQLInnerExecutor;
import com.tongji.hbase.migration.hsql.HSQLInnerScanner;
import com.tongji.hbase.migration.hsql.HSQLParser;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * Basic query executor.
 * 
 * @author Zhao Long
 */
public class BasicQueryInnerExecutor extends HSQLExecutorBasic implements
		HSQLInnerExecutor<HSQLInnerScanner> {

	static {
		HSQLConnection.registerQueryInnerExecutor(HSQL_WORD_SELECT + " \\* "
				+ HSQL_WORD_FROM
				+ " \\w+( WHERE ((\\w+=[\\d\\.]+)| AND |(\\w+='[^']*'))+)?",
				BasicQueryInnerExecutor.class);
	}

	@Override
	public HSQLInnerScanner execute(Result result, String mainTableName,
			String hsql) throws HSQLException {
		// TODO Auto-generated method stub
		HSQLParser parser = new HSQLParser(hsql);
		String tableName = parser.getTableName();

		HTableDef htd = null;
		try {
			htd = adapter.getHTableDefbyName(mainTableName);
		} catch (UnknownTableException ute) {
			// TODO Auto-generated catch block
			throw new HSQLException("Failed to Get the HTableDef of: "
					+ tableName, ute);
		}

		HSQLInnerScanner scanner = new HSQLInnerScanner(mainTableName,
				tableName, result);
		String keyColumnName = htd.getRowKey().getColumnName();
		Set<byte[]> columnNameSet = result.getNoVersionMap()
				.get(Bytes.toBytes(tableName)).keySet();
		for (byte[] columnName : columnNameSet) {
			if (keyColumnName.equals(HSQLHelper.getSubColumnName(columnName))) {
				byte[] columnId = HSQLHelper.getSubColumnId(columnName);
				boolean toAdd = true;
				for (Entry<String, Object> e : parser.getWherePairs()
						.entrySet()) {
					String parserColumName = e.getKey();
					Class<?> clazz = htd.getColumn(tableName, parserColumName)
							.getColumnType().getTypeClass();

					byte[] resultValue = HBaseUtil.getResultValue(result,
							tableName, HSQLHelper.buildSubColumnName(columnId,
									parserColumName));
					byte[] parserValue = null;
					try {
						parserValue = HBaseUtil.toBytes(clazz, e.getValue());
					} catch (IOException ioe) {
						// TODO Auto-generated catch block
						throw new HSQLException(
								"Failed to Get the Byte Value of: "
										+ e.getValue(), ioe);
					}
					if (!Bytes.equals(parserValue, resultValue)) {
						toAdd = false;
						break;
					}
				}
				if (toAdd) {
					addInnerScannerItem(scanner, columnId);
				}
			}
		}
		return scanner;
	}
}
