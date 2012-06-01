package storm.contrib.rdbms;

import java.sql.SQLException;
import java.util.ArrayList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/*
 * Sample Topology using the RDBMSDumperBolt
 * Its important to note that, the rdbms table column names should match the fields of the input stream tuples for the topology to work
 * For eg. the table used below if already created, should have word and number as the columns with resp. data types.
 */
public class RDBMSDumperTopology {

		public static void main(String[] args) throws SQLException {
			ArrayList<String> columnNames = new ArrayList<String>();
			ArrayList<String> columnTypes = new ArrayList<String>();
			String tableName = "testTable";
			// Note: if the rdbms table need not to have a primary key, set the variable 'primaryKey' to 'N/A'
			// else set its value to the name of the tuple field which is to be treated as primary key
			String primaryKey = "N/A";
			String rdbmsUrl = "jdbc:mysql://localhost:3306/testDB" ;
			String rdbmsUserName = "root"; 
			String rdbmsPassword = "root";
			
			//add the column names and the respective types in the two arraylists
			columnNames.add("word");
			columnNames.add("number");
			
			//add the types
			columnTypes.add("varchar (100)");
			columnTypes.add("int");
			
			TopologyBuilder builder = new TopologyBuilder();
			
			//set the spout for the topology
			builder.setSpout("spout", new SampleSpout(), 10);

			//dump the stream data into rdbms table		
			RDBMSDumperBolt dumperBolt = new RDBMSDumperBolt(primaryKey, tableName, columnNames, columnTypes, rdbmsUrl, rdbmsUserName, rdbmsPassword);
			builder.setBolt("dumperBolt",dumperBolt, 1).shuffleGrouping("spout");

			Config conf = new Config();
			conf.setDebug(true);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("rdbms-workflow", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.shutdown();
		}
	}

