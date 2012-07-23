package storm.contrib.hbase.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import storm.contrib.hbase.bolts.HBaseDumperBolt;
import storm.contrib.hbase.spouts.TestSpout;

/*
 * Topology that creates an hbase table (if not already created) with a single column family, row key being the timestamp
 */
public class HBaseDumperBoltTopology {
	public static void main(String[] args) {

		/*
		 * List of column family names of the hbase table
		 */
		ArrayList<String> columnFamilyNames = new ArrayList<String>();
		
		/*
		 * List containing list of column names per column family of the hbase table
		 */
		ArrayList<ArrayList<String>> columnNamesList  = new ArrayList<ArrayList<String>>();
		
		/*
		 * Temporary list used for populating the @columnNamesList
		 */
		ArrayList<String> columnNames = new ArrayList<String>();
		
		/*
		 * rowKey of the hbase table, assigned "timestamp" if none of the column's value but timestamp is to be used as the row key, else the column name
		 */
		String rowKey = null;
		
		/*
		 * hbase table name 
		 */
		String tableName = "testTable";
		
		/*
		 * local fs location of the hbase-site.xml file
		 */
		String hbaseXmlFileLocation = "path_to_hbase_setup/hbase-0.90.3-cdh3u1/conf/hbase-site.xml";
		
		TopologyBuilder builder = new TopologyBuilder();

		//add spout to the builder
		builder.setSpout("spout", new TestSpout(), 2);

		//populate the column family name and column names list
		columnFamilyNames.add("colFamily1");
		columnNames.add("column1");
		columnNames.add("column2");
		columnNamesList.add(columnNames);
		columnNames = new ArrayList<String>();
		//set rowKey = "timestamp" if value of none of the columns is to be used as rowKey
		rowKey = "timestamp";
		//if some column's value is to be used as the row key, for e.g. esperBoltOutputInteger then use 
		//rowKey = "outputInteger";

		//add dumper bolt to the builder
		HBaseDumperBolt dumperBolt = new HBaseDumperBolt(hbaseXmlFileLocation, tableName, rowKey, columnFamilyNames, columnNamesList);
		builder.setBolt("dumperBolt", dumperBolt, 1).shuffleGrouping("spout");

		Config conf = new Config();
		conf.setDebug(true);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sample-workflow", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.shutdown();
	}
}
