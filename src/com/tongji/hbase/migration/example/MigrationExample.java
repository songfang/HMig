package com.tongji.hbase.migration.example;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;

import com.tongji.hbase.migration.Adapter;
import com.tongji.hbase.migration.Convertor;
import com.tongji.hbase.migration.common.ConfigDefs;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.common.HTableDef.ColumnInfo;
import com.tongji.hbase.migration.common.TableDef;
import com.tongji.hbase.migration.dbvisualizer.XMLSchemaParser;
import com.tongji.hbase.migration.util.ResourceManager;

/**
 * The example of using whole migration tools.
 * 
 * @author Zhao Long
 */
public class MigrationExample implements ConfigDefs {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String name = Adapter.GLOBAL_NAME;
		System.out.println("Start to convert project " + name + "...");
		XMLSchemaParser parser = new XMLSchemaParser();
		try {
			parser.init(ResourceManager.getJarResourceAsStream(name + ".xml",
					XMLSchemaParser.class));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("Error! Failed to find file: " + name + ".xml");
			return;
		}
		List<TableDef> defs = null;
		try {
			defs = parser.parse();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println("Error! Failed to parse file: " + name + ".xml");
			return;
		}
		TableDef.buildAllRelations(defs);

		System.out.println("Get the following table schemas: ");
		for (TableDef def : defs) {
			System.out.println();
			printTableDef(def);
		}

		Convertor convertor = new Convertor(defs);
		System.out.println("Do basic convert...");
		try {
			convertor.basic();
		} catch (Exception e) {
			System.err.println("Error! Failed to do basic convert");
			return;
		}

		System.out.println("Do nest convert to table?");
		while (true) {
			String line = getInputLine();
			if (line.equals("")) {
				break;
			} else {
				try {
					convertor.nest(line.trim(), true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.err.println("Error! Failed to do nest convert to: "
							+ line);
				}
			}
		}

		System.out.println("Do inline convert to table?");
		while (true) {
			String line = getInputLine();
			if (line.equals("")) {
				break;
			} else {
				String[] tableNames = line.trim().split(" ");
				try {
					convertor.inline(tableNames[0], tableNames[1], true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.err
							.println("Error! Failed to do inline convert to: "
									+ line);
				}
			}
		}

		System.out.println("Do split convert to table?");
		while (true) {
			String line = getInputLine();
			if (line.equals("")) {
				break;
			} else {
				try {
					convertor.split(line.trim(), false);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.err.println("Error! Failed to do split convert to: "
							+ line);
				}
			}
		}

		Adapter adapter = convertor.getAdapter();
		try {
			adapter.toXML();
			adapter = Adapter.fromXML();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("Error! Failed to save the definition file");
		}

		System.out
				.println("Finish and get the following HBase table schemas: ");
		for (HTableDef hdef : adapter.getHTableDefs().values()) {
			System.out.println();
			printHTableDef(hdef);
		}
	}

	private static String getInputLine() {
		Scanner scanner = new Scanner(System.in);
		String input = scanner.nextLine();
		return input;
	}

	private static void printTableDef(TableDef def) {
		System.out.println("--- " + def.getTableName() + " ---");
		for (Entry<String, String> e : def.getColumnKeys().entrySet()) {
			System.out.format("%-20s %-15s\n", e.getKey(), e.getValue());
		}
		System.out.format("%-20s %-15s\n", "*Primary Key:",
				def.getPrimaryKeys()[0]);
		System.out.format("%-20s %-15s\n", "*Has One:", def
				.getHasOneRefTables().keySet());
		System.out.format("%-20s %-15s\n", "*Has Many:", def
				.getHasManyRefTables().keySet());
	}

	private static void printHTableDef(HTableDef def) {
		System.out.println("--- " + def.getTableName() + " ---");
		for (String cf : def.getColumnFamilies()) {
			for (ColumnInfo ci : def.getColumns(cf)) {
				System.out.format("%-30s %-15s\n",
						cf + ":" + ci.getColumnName(), ci.getColumnName());
			}
		}
	}
}
