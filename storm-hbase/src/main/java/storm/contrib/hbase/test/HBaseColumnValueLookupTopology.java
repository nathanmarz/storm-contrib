package storm.contrib.hbase.test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import storm.contrib.hbase.bolts.HBaseColumnValueLookUpBolt;
import storm.contrib.hbase.spouts.RowKeyEmitterSpout;

public class HBaseColumnValueLookupTopology {
	public static void main(String[] args) {
		
		/*
		 * local fs location of the hbase-site.xml file
		 */
		String hbaseXmlFileLocation = "path_to_hbase_setup/hbase-0.90.3-cdh3u1/conf/hbase-site.xml";
		
		TopologyBuilder builder = new TopologyBuilder();

		//add spout to the topology which emits the hbase table row key value.
		builder.setSpout("spout", new RowKeyEmitterSpout(), 2);

		String tableName = "hbaseBoltTestTable", rowKeyFieldName = "word";
		String columnFamilyName = "colFamily1", columnName = "column1";
		HBaseColumnValueLookUpBolt hbaseLookUpBolt = new HBaseColumnValueLookUpBolt(hbaseXmlFileLocation, rowKeyFieldName, tableName, columnFamilyName, columnName); 
		
		// add the hbase lookup bolt to the topology.
		builder.setBolt("hbaseLookUpBolt", hbaseLookUpBolt, 1).shuffleGrouping("spout");

		Config conf = new Config();
		conf.setDebug(true);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sample-workflow", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.shutdown();
	}

}
