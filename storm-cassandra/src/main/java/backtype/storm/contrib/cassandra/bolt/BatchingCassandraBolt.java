package backtype.storm.contrib.cassandra.bolt;

import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.determinable.ColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultColumnFamilyDeterminable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

@SuppressWarnings("serial")
public abstract class BatchingCassandraBolt extends AbstractBatchingBolt implements
		CassandraConstants {
	private static final Logger LOG = LoggerFactory
			.getLogger(BatchingCassandraBolt.class);

	public static enum AckStrategy {
		ACK_IGNORE, ACK_ON_RECEIVE, ACK_ON_WRITE;
	}

	protected AckStrategy ackStrategy = AckStrategy.ACK_IGNORE;

	protected OutputCollector collector;

	private Fields declaredFields;

	private String cassandraHost;
	private String cassandraPort;
	private String cassandraKeyspace;

	protected Cluster cluster;
	protected Keyspace keyspace;

	protected ColumnFamilyDeterminable cfDeterminable;
//	protected RowKeyDeterminable rkDeterminable; 

	public BatchingCassandraBolt(String columnFamily) {
		this(new DefaultColumnFamilyDeterminable(columnFamily));
	}

	public BatchingCassandraBolt(ColumnFamilyDeterminable cfDeterminable) {
		this.cfDeterminable = cfDeterminable;
//		this.rkDeterminable = rkDeterminable;
	}

	public void setAckStrategy(AckStrategy strategy) {
		this.ackStrategy = strategy;
	}

	/*
	 * IRichBolt Implementation
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		LOG.debug("Preparing...");
		this.cassandraHost = (String) stormConf.get(CASSANDRA_HOST);
		this.cassandraKeyspace = (String) stormConf.get(CASSANDRA_KEYSPACE);
		this.cassandraPort = String.valueOf(stormConf.get(CASSANDRA_PORT));

		this.collector = collector;

		initCassandraConnection();

		if (this.ackStrategy == AckStrategy.ACK_ON_RECEIVE) {
			super.setAckOnReceive(true);
		}

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
	public void cleanup() {
		super.cleanup();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if (this.declaredFields != null) {
			declarer.declare(this.declaredFields);
		}

	}

}
