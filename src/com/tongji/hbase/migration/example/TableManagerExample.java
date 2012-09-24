package com.tongji.hbase.migration.example;

import com.tongji.hbase.migration.TableManager;

/**
 * The example of using TableManager.
 * 
 * @author Zhao Long
 */
public class TableManagerExample {

	public static void main(String[] args) {
		if (args == null || args.length != 1) {
			System.err.println("Wrong command.");
			return;
		}

		String command = args[0].trim();
		try {
			TableManager tm = new TableManager();
			if (command.equals("clean")) {
				tm.deleteAllHTables();
			} else if (command.equals("create")) {
				tm.recreateAllHTables();
			} else if (command.equals("migrate")) {
				tm.migrateAllHTables();
			}
			System.out.println("Finished.");
			tm.destroy();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println("Error! Faild to create TableManager.");
			return;
		}
	}
}
