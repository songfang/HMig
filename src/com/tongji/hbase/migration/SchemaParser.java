package com.tongji.hbase.migration;

import java.io.InputStream;
import java.util.List;

import com.tongji.hbase.migration.common.TableDef;

/**
 * Tool interface to parse the table schema.
 * 
 * @author Zhao Long
 */
public interface SchemaParser {

	/**
	 * Initialize the parser.
	 * 
	 * @param in
	 *            the input stream of the schema
	 */
	public void init(InputStream in);

	/**
	 * Parse the schema.
	 * 
	 * @return definitions of tables parsed
	 * @throws Exception when failed to parse the schema
	 */
	public List<TableDef> parse() throws Exception;

}
