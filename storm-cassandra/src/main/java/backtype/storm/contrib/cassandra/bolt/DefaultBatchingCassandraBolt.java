package backtype.storm.contrib.cassandra.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.cassandra.bolt.BatchingCassandraBolt.AckStrategy;
import backtype.storm.contrib.cassandra.bolt.determinable.ColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultColumnFamilyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.DefaultRowKeyDeterminable;
import backtype.storm.contrib.cassandra.bolt.determinable.RowKeyDeterminable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class DefaultBatchingCassandraBolt extends BatchingCassandraBolt implements
		CassandraConstants {
    private RowKeyDeterminable rkDeterminable;
	public DefaultBatchingCassandraBolt(
                    ColumnFamilyDeterminable cfDeterminable,
                    RowKeyDeterminable rkDeterminable) {
        super(cfDeterminable);
        this.rkDeterminable = rkDeterminable;
    }

    public DefaultBatchingCassandraBolt(String columnFamily, String rowKey) {
        this(new DefaultColumnFamilyDeterminable(columnFamily), new DefaultRowKeyDeterminable(rowKey));
    }

    private static final Logger LOG = LoggerFactory
			.getLogger(DefaultBatchingCassandraBolt.class);


    @Override
    public void executeBatch(List<Tuple> inputs) {
        ArrayList<Tuple> tuplesToAck = new ArrayList<Tuple>();
        try {
            Mutator<String> mutator = HFactory.createMutator(this.keyspace,
                    new StringSerializer());
            for (Tuple input : inputs) {
                String columnFamily = this.cfDeterminable
                        .determineColumnFamily(input);
                Object rowKey = this.rkDeterminable.determineRowKey(input);
                Fields fields = input.getFields();
                for (int i = 0; i < fields.size(); i++) {
                    mutator.addInsertion(rowKey.toString(), columnFamily,
                            HFactory.createStringColumn(fields.get(i), input
                                    .getValue(i).toString()));
                    tuplesToAck.add(input);
                }
            }
            mutator.execute();

        } catch (Throwable e) {
            LOG.warn("Unable to write batch.", e);
        } finally {
            if (this.ackStrategy == AckStrategy.ACK_ON_WRITE) {
                for (Tuple tupleToAck : tuplesToAck) {
                    this.collector.ack(tupleToAck);
                }
            }
        }

    }


}
