package com.tongji.hbase.migration.example;

import java.util.List;

import com.tongji.hbase.migration.Adapter;
import com.tongji.hbase.migration.Convertor;
import com.tongji.hbase.migration.common.HTableDef;
import com.tongji.hbase.migration.common.TableDef;
import com.tongji.hbase.migration.dbvisualizer.XMLSchemaParser;
import com.tongji.hbase.migration.util.ResourceManager;

/**
 * The example of Adapter.
 * 
 * @author Zhao Long
 */
public class ConvertorExample extends Adapter {

	public static void main(String[] args) throws Exception {
		XMLSchemaParser parser = new XMLSchemaParser();
		parser.init(ResourceManager.getJarResourceAsStream(
				GLOBAL_NAME + ".xml", XMLSchemaParser.class));
		List<TableDef> defs = parser.parse();
		TableDef.buildAllRelations(defs);

		Convertor convertor = new Convertor(defs);
		convertor.basic();
		// convertor.nest("House", true);
		convertor.nest("Model", true);
		convertor.nest("Color", true);
		convertor.inline("GoodsRelated", "Goods", true);
		convertor.split("Picture", false);

		Adapter adapter = convertor.getAdapter();

		adapter.toXML();

		adapter = Adapter.fromXML();

		for (HTableDef hdef : adapter.getHTableDefs().values()) {
			System.out.println("-----");
			System.out.println(hdef);
		}

		System.out.println("*****");
		System.out.println(adapter.getRelationMappings());
	}

}
