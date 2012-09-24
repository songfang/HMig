package com.tongji.hbase.migration.hsql;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

import com.tongji.hbase.migration.common.HSQLException;

/**
 * Tool to parse HSQL.
 * 
 * @author DemonDeath
 */
public class HSQLParser {

	protected Statement statemant;

	protected String tableName;
	protected List<String> selectItems;
	protected Map<String, Object> wherePairs;

	/**
	 * Constructor.
	 * 
	 * @throws HSQLException
	 *             when failed to parse HSQL
	 */
	public HSQLParser(String hsql) throws HSQLException {
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		try {
			statemant = parserManager.parse(new StringReader(hsql));
			statemant.accept(new HSQLVisitor());
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			throw new HSQLException("Failed to Parse HSQL: " + hsql, e);
		}
	}

	/**
	 * Get the table name.
	 * 
	 * @return the table name
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Get the selected item names.
	 * 
	 * @return the item names
	 */
	public String[] getSelectItems() {
		return selectItems.toArray(new String[] {});
	}

	/**
	 * Get the where pairs.
	 * 
	 * @return the where pairs
	 */
	public Map<String, Object> getWherePairs() {
		return wherePairs;
	}

	// The HSQL visitor.
	private class HSQLVisitor implements FromItemVisitor, SelectVisitor,
			StatementVisitor {

		@Override
		public void visit(Select select) {
			// TODO Auto-generated method stub
			select.getSelectBody().accept(this);
		}

		@Override
		public void visit(Delete delete) {
			// TODO Auto-generated method stub

		}

		@Override
		public void visit(Update update) {
			// TODO Auto-generated method stub

		}

		@Override
		public void visit(Insert insert) {
			// TODO Auto-generated method stub

		}

		@Override
		public void visit(Replace replace) {
			// TODO Auto-generated method stub

		}

		@Override
		public void visit(Drop drop) {
			// TODO Auto-generated method stub

		}

		@Override
		public void visit(Truncate truncate) {
			// TODO Auto-generated method stub

		}

		@Override
		public void visit(CreateTable createTable) {
			// TODO Auto-generated method stub

		}

		@Override
		public void visit(PlainSelect plainSelect) {
			// TODO Auto-generated method stub
			selectItems = new ArrayList<String>();
			for (Object o : plainSelect.getSelectItems()) {
				if (o instanceof AllColumns) {
					break;
				} else if (o instanceof SelectExpressionItem) {
					SelectExpressionItem item = (SelectExpressionItem) o;
					selectItems.add(item.toString());
				}
			}

			plainSelect.getFromItem().accept(this);

			wherePairs = new HashMap<String, Object>();
			parseExpression(plainSelect.getWhere());
		}

		@Override
		public void visit(Union union) {
			// TODO Auto-generated method stub

		}

		@Override
		public void visit(Table table) {
			// TODO Auto-generated method stub
			tableName = table.getWholeTableName();
		}

		@Override
		public void visit(SubSelect subSelect) {
			// TODO Auto-generated method stub

		}

		@Override
		public void visit(SubJoin subJoin) {
			// TODO Auto-generated method stub

		}

		// Parse the BinaryExpression.
		private void parseBinaryExpression(BinaryExpression expression) {
			String name = expression.getLeftExpression().toString();
			Object value = null;
			Expression rightExpression = expression.getRightExpression();
			if (rightExpression instanceof StringValue) {
				value = ((StringValue) rightExpression).getValue();
			} else if (rightExpression instanceof LongValue) {
				Long longValue = new Long(((LongValue) rightExpression).getValue());
				value = new Integer(longValue.intValue());
			} else if (rightExpression instanceof DoubleValue) {
				value = new Double(((DoubleValue) rightExpression).getValue());
			}
			wherePairs.put(name, value);
		}

		// Parse the AndExpression.
		private void parseAndExpression(AndExpression expression) {
			parseExpression(expression.getLeftExpression());
			parseExpression(expression.getRightExpression());
		}

		// Parse the Expression.
		private void parseExpression(Expression expression) {
			if (expression instanceof AndExpression) {
				parseAndExpression((AndExpression) expression);
			} else if (expression instanceof BinaryExpression) {
				parseBinaryExpression((BinaryExpression) expression);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		HSQLParser parser = new HSQLParser(
				"SELECT * FROM MY_TABLE1 WHERE a=1 AND b=1 AND c=5");
		System.out.println(parser.tableName);
		System.out.println(parser.selectItems);
		System.out.println(parser.wherePairs);
		Long l = new Long(100L);
		System.out.println(l.intValue());
	}
}
