package com.tongji.hbase.migration.example;

import com.tongji.hbase.migration.common.HSQLException;
import com.tongji.hbase.migration.hsql.HSQLConnection;
import com.tongji.hbase.migration.hsql.HSQLInnerResult;
import com.tongji.hbase.migration.hsql.HSQLInnerScanner;
import com.tongji.hbase.migration.hsql.HSQLResult;
import com.tongji.hbase.migration.hsql.HSQLScanner;

/**
 * The example of HSQL.
 * 
 * @author Zhao Long
 */
public class HSQLExample {

	public static void main(String[] args) {
		HSQLScanner scanner = null;
		HSQLInnerScanner innerScanner = null;
		HSQLConnection connection = new HSQLConnection();

		System.out.println("** Example 1 **");
		try {
			scanner = connection.executeQuery("select * from Brand");
			System.out.println("brandID\tbrandName\tbrandCreateDate");
			System.out.println("-------\t---------\t---------------");
			for (HSQLResult result : scanner) {
				String line = result.getInt("brandID") + "\t";
				line += result.getString("brandName") + "\t";
				line += result.getDate("brandCreateDate") + "\t";
				System.out.println(line);
			}
		} catch (HSQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (scanner != null) {
				scanner.close();
			}
		}
		System.out.println();

		System.out.println("** Example 2 **");
		try {
			scanner = connection
					.executeQuery("select brandID,brandName from Brand");
			System.out.println("brandID\tbrandName");
			System.out.println("-------\t---------");
			for (HSQLResult result : scanner) {
				String line = result.getInt("brandID") + "\t";
				line += result.getString("brandName") + "\t";
				System.out.println(line);
			}
		} catch (HSQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (scanner != null) {
				scanner.close();
			}
		}
		System.out.println();

		System.out.println("** Example 3 **");
		try {
			HSQLResult result = connection
					.executeGet("select * from Brand where brandID=183");
			System.out.println("brandID\tbrandName\tbrandCreateDate");
			System.out.println("-------\t---------\t---------------");
			String line = result.getInt("brandID") + "\t";
			line += result.getString("brandName") + "\t";
			line += result.getDate("brandCreateDate") + "\t";
			System.out.println(line);
		} catch (HSQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println();

		System.out.println("** Example 4 **");
		try {
			HSQLResult result = connection
					.executeGet("select brandID,brandCreateDate from Brand where brandID=183");
			System.out.println("brandID\tbrandCreateDate");
			System.out.println("-------\t---------------");
			String line = result.getInt("brandID") + "\t";
			line += result.getDate("brandCreateDate") + "\t";
			System.out.println(line);
		} catch (HSQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println();

		System.out.println("** Example 5 **");
		try {
			scanner = connection
					.executeQuery("select * from Brand where brandName='∂‡¿÷ ø' and brandID=53");
			System.out.println("brandID\tbrandName\tbrandCreateDate");
			System.out.println("-------\t---------\t---------------");
			for (HSQLResult result : scanner) {
				String line = result.getInt("brandID") + "\t";
				line += result.getString("brandName") + "\t";
				line += result.getDate("brandCreateDate") + "\t";
				System.out.println(line);
			}
		} catch (HSQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (scanner != null) {
				scanner.close();
			}
		}
		System.out.println();

		System.out.println("** Example 6 **");
		try {
			HSQLResult result = connection.executeGet(
					"select * from Goods where goodsID=110",
					new String[] { "Color" });
			System.out.println("goodsID\tgoodsName\tgoodsType");
			System.out.println("-------\t---------\t---------");
			String line = result.getInt("goodsID") + "\t";
			line += result.getString("goodsName") + "\t";
			line += result.getString("goodsType") + "\t";
			System.out.println(line);

			try {
				innerScanner = result
						.executeQuery("select * from Color where colorID=113");
				System.out.println();
				System.out.println("colorID\tcolorName\tisDefault");
				System.out.println("-------\t---------\t---------");
				for (HSQLInnerResult innerResult : innerScanner) {
					String innerLine = innerResult.getInt("colorID") + "\t";
					innerLine += innerResult.getString("colorName") + "\t";
					innerLine += innerResult.getBoolean("isDefault") + "\t";
					System.out.println(innerLine);
				}
			} finally {
				// TODO: handle exception
				if (innerScanner != null) {
					innerScanner.close();
				}
			}
		} catch (HSQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (scanner != null) {
				scanner.close();
			}
		}
		System.out.println();
	}
}
