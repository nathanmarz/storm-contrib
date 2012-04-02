package backtype.storm.contrib.cassandra.bolt;

import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.determinable.ColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultRowKeyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.RowKeyDeterminable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class CassandraBolt implements IRichBolt, CassandraConstants {
	private static final Logger LOG = LoggerFactory
			.getLogger(CassandraBolt.class);

	private OutputCollector collector;
	private boolean autoAck = true;

	private Fields declaredFields;

	private String cassandraHost;
	private String cassandraPort;
	private String cassandraKeyspace;

	private Cluster cluster;
	private Keyspace keyspace;

	private ColumnFamilyDeterminable cfDeterminable;
	private RowKeyDeterminable rkDeterminable;

	public CassandraBolt(String columnFamily, String rowkeyField) {

		this(new DefaultColumnFamilyDeterminable(columnFamily),
				new DefaultRowKeyDeterminable(rowkeyField));

	}

	public CassandraBolt(ColumnFamilyDeterminable cfDeterminable,
			RowKeyDeterminable rkDeterminable) {
		this.cfDeterminable = cfDeterminable;
		this.rkDeterminable = rkDeterminable;
	}

	/*
	 * IRichBolt Implementation
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		LOG.debug("Preparing...");
		this.cassandraHost = (String) stormConf.get(CASSANDRA_HOST);
		this.cassandraKeyspace = (String) stormConf.get(CASSANDRA_KEYSPACE);
		this.cassandraPort = String.valueOf(stormConf.get(CASSANDRA_PORT));

		this.collector = collector;

		initCassandraConnection();

	}

	private void initCassandraConnection() {
		// setup Cassandra connection
		try {
			this.cluster = HFactory.getOrCreateCluster("cassandra-bolt",
					new CassandraHostConfigurator(this.cassandraHost + ":"
							+ this.cassandraPort));
			this.keyspace = HFactory.createKeyspace(this.cassandraKeyspace,
					this.cluster);
		} catch (Throwable e) {
			LOG.warn("Preparation failed.", e);
			throw new IllegalStateException("Failed to prepare CassandraBolt",
					e);
		}
	}

	@Override
	public void execute(Tuple input) {
		LOG.debug("Tuple received: " + input);
		try {
			String columnFamily = this.cfDeterminable
					.determineColumnFamily(input);
			Object rowKey = this.rkDeterminable.determineRowKey(input);

			Mutator<String> mutator = HFactory.createMutator(this.keyspace,
					new StringSerializer());
			Fields fields = input.getFields();

			for (int i = 0; i < fields.size(); i++) {
				// LOG.debug("Name: " + fields.get(i) + ", Value: "
				// + input.getValue(i));
				mutator.addInsertion(rowKey.toString(), columnFamily, HFactory
						.createStringColumn(fields.get(i), input.getValue(i)
								.toString()));
			}
			mutator.execute();

			if (this.autoAck) {
				this.collector.ack(input);
			}
		} catch (Throwable e) {
			LOG.warn("Caught throwable.", e);
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (this.declaredFields != null) {
			declarer.declare(this.declaredFields);
		}

	}

	public boolean isAutoAck() {
		return autoAck;
	}

	public void setAutoAck(boolean autoAck) {
		this.autoAck = autoAck;
	}

}
