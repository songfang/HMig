package com.tongji.hbase.migration.dbvisualizer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.dom4j.Element;
import org.dom4j.VisitorSupport;
import org.dom4j.io.SAXReader;

import com.tongji.hbase.migration.SchemaParser;
import com.tongji.hbase.migration.common.Config;
import com.tongji.hbase.migration.common.TableDef;
import com.tongji.hbase.migration.common.UnknownKeyException;
import com.tongji.hbase.migration.util.Logger;

/**
 * Tool to parse the XML file defining table schema.
 * 
 * @author Zhao Long
 */
public class XMLSchemaParser implements SchemaParser {

	private static final Logger log = Config.getLogger(XMLSchemaParser.class);

	private static final String NODE_TABLE = "TABLE";
	private static final String NODE_TABLE_NAME = "NAME";
	private static final String NODE_COLUMNS = "COLUMNS";
	private static final String NODE_TABLE_COLUMN = "COLUMN";
	private static final String NODE_COLUMN_NAME = "NAME";
	private static final String NODE_COLUMN_DATA_TYPE = "DATA_TYPE";
	private static final String NODE_CONSTRAINTS = "CONSTRAINTS";
	private static final String NODE_TABLE_CONSTRAINT = "CONSTRAINT";
	private static final String NODE_CONSTRAINT_TYPE = "TYPE";

	private static final String NODE_FLAG_PK = "PRIMARY KEY";

	private InputStream in;
	private List<TableDef> tableDefs;

	@Override
	public void init(InputStream in) {
		// TODO Auto-generated method stub
		this.in = in;
		this.tableDefs = new ArrayList<TableDef>();
	}

	@Override
	public List<TableDef> parse() throws Exception {
		// TODO Auto-generated method stub
		SAXReader reader = new SAXReader();
		Element root = reader.read(in).getRootElement();

		for (Object o : root.elements(NODE_TABLE)) {
			Element tableNode = (Element) o;
			TableDef tableDef = new TableDef(getNodeText(tableNode,
					NODE_TABLE_NAME));

			tableNode.element(NODE_COLUMNS).accept(
					new XMLColumnVisitor(tableDef));
			tableNode.element(NODE_CONSTRAINTS).accept(
					new XMLConstraintVisitor(tableDef));
			this.tableDefs.add(tableDef);
		}

		return this.tableDefs;
	}

	// Get the text of the node by name.
	private static String getNodeText(Element tableNode, String name) {
		return tableNode.element(name).getTextTrim();
	}

	// Visitor to parse the columns XML document.
	private class XMLColumnVisitor extends VisitorSupport {

		private TableDef tableDef;

		/**
		 * Constructor.
		 * 
		 * @param tableDef
		 *            the table definition
		 */
		public XMLColumnVisitor(TableDef tableDef) {
			this.tableDef = tableDef;
		}

		@Override
		public void visit(Element node) {
			if (NODE_TABLE_COLUMN.equals(node.getName())) {
				String name = getNodeText(node, NODE_COLUMN_NAME);
				String type = getNodeText(node, NODE_COLUMN_DATA_TYPE);
				try {
					tableDef.addKey(name, type);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					log.warn("Failed to Parse Column: " + name, e);
				}
			}
		}
	}

	// Visitor to parse the constraints XML document.
	private class XMLConstraintVisitor extends VisitorSupport {

		private TableDef tableDef;

		/**
		 * Constructor.
		 * 
		 * @param tableDef
		 *            the table definition
		 */
		public XMLConstraintVisitor(TableDef tableDef) {
			this.tableDef = tableDef;
		}

		@Override
		public void visit(Element node) {
			if (NODE_TABLE_CONSTRAINT.equals(node.getName())) {
				String type = getNodeText(node, NODE_CONSTRAINT_TYPE);

				if (NODE_FLAG_PK.equals(type)) {
					parsePrimaryKeys(node.element(NODE_COLUMNS));
				}
			}
		}

		// Parse primary keys in the table.
		private void parsePrimaryKeys(Element node) {
			for (Object o : node.elements(NODE_TABLE_COLUMN)) {
				Element columnNode = (Element) o;
				String name = getNodeText(columnNode, NODE_COLUMN_NAME);
				try {
					this.tableDef.addPrimaryKey(name);
				} catch (UnknownKeyException e) {
					// TODO Auto-generated catch block
					log.warn(
							"Failed to Add Primary Key to Table: "
									+ tableDef.getTableName(), e);
				}
			}
		}
	}
}
