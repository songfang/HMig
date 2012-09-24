package com.tongji.hbase.migration.hsql.executor;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.tongji.hbase.migration.hsql.HSQLConnection;
import com.tongji.hbase.migration.hsql.HSQLResult;
import com.tongji.hbase.migration.hsql.HSQLScanner;
import com.tongji.hbase.migration.util.HBaseUtil;

/**
 * The basic scanner.
 * 
 * @author Zhao Long
 */
public class BasicScanner implements HSQLScanner {

	private HSQLResultIterator iterator;
	private ResultScanner scanner;
	private String tableName;
	private HTable table;

	/**
	 * Constructor. <b>Do NOT call manually.</b>
	 * 
	 * @param table
	 *            the HTable
	 * @param tableName
	 *            the table name
	 * @param scanner
	 *            the ResultScanner
	 */
	public BasicScanner(HTable table, String tableName, ResultScanner scanner) {
		this.table = table;
		this.tableName = tableName;
		this.scanner = scanner;
		this.iterator = new HSQLResultIterator();
	}

	@Override
	public Iterator<HSQLResult> iterator() {
		// TODO Auto-generated method stub
		return new HSQLResultIterator();
	}

	@Override
	public HSQLResult next() {
		// TODO Auto-generated method stub
		return this.iterator.next();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		HBaseUtil.close(this.scanner);
		HSQLConnection.putHTable(this.table);
	}

	// Inner Iterator Class.
	private class HSQLResultIterator implements Iterator<HSQLResult> {

		private Iterator<Result> iterator;

		// Constructor.
		public HSQLResultIterator() {
			this.iterator = scanner.iterator();
		}

		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return this.iterator.hasNext();
		}

		@Override
		public HSQLResult next() {
			// TODO Auto-generated method stub
			Result result = this.iterator.next();
			if (result == null) {
				return null;
			} else {
				return new HSQLResult(tableName, result);
			}
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			this.iterator.remove();
		}
	}
}
