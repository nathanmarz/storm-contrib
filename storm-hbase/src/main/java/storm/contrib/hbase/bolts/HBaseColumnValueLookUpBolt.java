package storm.contrib.hbase.bolts;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import storm.contrib.hbase.utils.HBaseCommunicator;
import storm.contrib.hbase.utils.HBaseConnector;

/*
 * Reads the specified column of HBase table and emits the row key and the column values in the form of tuples
 */
public class HBaseColumnValueLookUpBolt implements IBasicBolt {
	
	private static final long serialVersionUID = 1L;
	
	private String tableName = null, colFamilyName = null, colName = null, rowKeyField = null, columnValue = null;
	
	private static transient HBaseConnector connector = null;
	private static transient HBaseConfiguration conf = null;
	private static transient HBaseCommunicator communicator = null;
	OutputCollector _collector;

	/*
	 * Constructor initializes the variables storing the hbase table information and connects to hbase
	 */
	public HBaseColumnValueLookUpBolt(final String hbaseXmlLocation, final String rowKeyField, final String tableName, final String colFamilyName, final String colName) {

		this.tableName = tableName;
		this.colFamilyName = colFamilyName;
		this.colName = colName;
		this.rowKeyField = rowKeyField;

		connector = new HBaseConnector();
		conf = connector.getHBaseConf(hbaseXmlLocation);
		communicator = new HBaseCommunicator(conf);
	}

	/*
	 * emits the value of the column with name @colName and rowkey @rowKey
	 * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {

		String rowKey = input.getStringByField(this.rowKeyField);
		columnValue = communicator.getColEntry(this.tableName, rowKey, this.colFamilyName, this.colName);
		collector.emit(tuple(rowKey, columnValue));
	}

	public void prepare(Map confMap, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rowKey", "columnValue"));
	}

	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> map = null;
		return map;
	}

	public void prepare(Map stormConf, TopologyContext context) {
	}
}

