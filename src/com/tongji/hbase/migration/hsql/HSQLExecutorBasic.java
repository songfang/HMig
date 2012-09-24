package com.tongji.hbase.migration.hsql;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.tongji.hbase.migration.common.HSQLException;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * Tool to provide basic method to executors.
 * 
 * @author Zhao Long
 */
public abstract class HSQLExecutorBasic implements HSQLCommon {

	private static final SimpleDateFormat sdf;

	static {
		sdf = new SimpleDateFormat(HSQL_FORMAT_DATE);
	}

	// Get ResultScanner of given table.
	protected static ResultScanner getScanner(HTable table, Scan scan)
			throws HSQLException {
		try {
			return table.getScanner(scan);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new HSQLException("Failed to Get Scanner of Table: "
					+ Bytes.toString(table.getTableName()), e);
		}
	}

	// Get Result of given table.
	protected static Result getResult(HTable table, Get get)
			throws HSQLException {
		try {
			return table.get(get);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new HSQLException("Failed to Result of Table: "
					+ Bytes.toString(table.getTableName()), e);
		}
	}

	// To Get The byte value of the given value.
	protected static byte[] toBytes(Class<?> clazz, Object val)
			throws HSQLException {
		try {
			if (Date.class.equals(clazz)) {
				val = sdf.parse((String) val);
			}
			return HBaseUtil.toBytes(clazz, val);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new HSQLException("Failed to Convert the Value: " + val, e);
		}
	}

	// To add the item into the inner scanner.
	protected static boolean addInnerScannerItem(HSQLInnerScanner scanner,
			byte[] item) {
		return scanner.idList.add(item);
	}
}
