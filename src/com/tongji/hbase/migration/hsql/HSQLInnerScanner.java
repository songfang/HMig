package com.tongji.hbase.migration.hsql;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.tongji.hbase.migration.common.Config;
import com.tongji.hbase.migration.common.HSQLException;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.common.HTableDef.ColumnInfo;
import com.tongji.hbase.migration.common.UnknownTableException;
import com.tongji.hbase.migration.util.Logger;

/**
 * Interface of inner scanner.
 * 
 * @author Zhao Long
 */
public class HSQLInnerScanner implements HSQLCommon, Closeable,
		Iterable<HSQLInnerResult> {

	private static final Logger log = Config.getLogger(HSQLInnerScanner.class);

	protected Result result;
	protected String mainTableName;
	protected String subTableName;
	protected List<byte[]> idList;
	protected HSQLInnerResultIterator iterator;

	/**
	 * Constructor. <b>Do NOT call manually.</b>
	 * 
	 * @param mainTableName
	 *            the main table name
	 * @param subTableName
	 *            the sub table name
	 * @param result
	 *            the Result
	 */
	public HSQLInnerScanner(String mainTableName, String subTableName,
			Result result) {
		this.mainTableName = mainTableName;
		this.subTableName = subTableName;
		this.result = result;
		this.idList = new ArrayList<byte[]>();
		this.iterator = new HSQLInnerResultIterator();
	}

	/**
	 * Grab the next inner result's worth of values.
	 * 
	 * @return the next HSQLInnerResult
	 * @throws HSQLException
	 *             when error occurs
	 */
	public HSQLInnerResult next() throws HSQLException {
		return this.iterator.next();
	}
	
	@Override
	public Iterator<HSQLInnerResult> iterator() {
		// TODO Auto-generated method stub
		return new HSQLInnerResultIterator();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	// Inner Iterator Class.
	private class HSQLInnerResultIterator implements Iterator<HSQLInnerResult> {

		private Iterator<byte[]> iterator;

		// Constructor.
		public HSQLInnerResultIterator() {
			this.iterator = idList.iterator();
		}

		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return this.iterator.hasNext();
		}

		@Override
		public HSQLInnerResult next() {
			// TODO Auto-generated method stub
			byte[] byteId = null;
			try {
				byteId = this.iterator.next();
			} catch (NoSuchElementException e) {
				return null;
			}

			HTableDef htd = null;
			try {
				htd = adapter.getHTableDefbyName(mainTableName);
			} catch (UnknownTableException e) {
				// TODO Auto-generated catch block
				log.warn("Failed to Get the HTableDef", e);
				return null;
			}

			HSQLInnerResult innerResult = new HSQLInnerResult();
			for (ColumnInfo ci : htd.getColumns(subTableName)) {
				String columnName = ci.getColumnName();
				byte[] subColumnName = HSQLHelper.buildSubColumnName(byteId,
						Bytes.toBytes(columnName));
				byte[] byteVal = result.getValue(Bytes.toBytes(subTableName),
						subColumnName);
				if (byteVal != null) {
					innerResult.valueMap.put(columnName, byteVal);
				}
			}
			return innerResult;
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			this.iterator.remove();
		}
	}
}
