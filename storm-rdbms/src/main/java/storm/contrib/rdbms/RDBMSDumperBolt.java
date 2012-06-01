package storm.contrib.rdbms;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;


public class RDBMSDumperBolt  implements IBasicBolt {
	private static final long serialVersionUID = 1L;
	private static transient RDBMSConnector connector = new RDBMSConnector();
	private static transient Connection con = null;
	private static transient RDBMSCommunicator communicator = null;
	private String tableName = null, primaryKey = null, dBUrl = null, password = null, username = null;
	private ArrayList<String> columnNames = new ArrayList<String>();
	private ArrayList<String> columnTypes = new ArrayList<String>();
	private ArrayList<Object> fieldValues = new ArrayList<Object>();

	
	public RDBMSDumperBolt(String primaryKey, String tableName, ArrayList<String> columnNames,
			ArrayList<String> columnTypes, String dBUrl, String username, String password) throws SQLException {
		super();
		this.primaryKey = primaryKey;
		this.tableName = tableName;
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.dBUrl = dBUrl;
		this.username = username;
		this.password = password;
	
		try {
			con = connector.getConnection(dBUrl, username, password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		communicator = new RDBMSCommunicator(con, primaryKey, tableName, columnNames, columnTypes);
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		fieldValues = new ArrayList<Object>();
		Object fieldValueObject;
		//add all the tuple values to a list 
		for (int i = 0; i < columnNames.size(); i++) {
			fieldValueObject = input.getValue(i);
			fieldValues.add(fieldValueObject);
		}
		//list passed as an argument to the insertRow funtion
		try {
			communicator.insertRow(fieldValues);
		} catch (SQLException e) {
			System.out.println("Exception occurred in adding a row ");			
			e.printStackTrace();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
}

