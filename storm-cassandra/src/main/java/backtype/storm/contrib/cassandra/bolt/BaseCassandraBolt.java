package backtype.storm.contrib.cassandra.bolt;

import java.util.Map;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;

@SuppressWarnings("serial")
public abstract class BaseCassandraBolt implements IBasicBolt,
                CassandraConstants {

    private static final Logger LOG = LoggerFactory
                    .getLogger(BaseCassandraBolt.class);

    private String cassandraHost;
    private String cassandraPort;
    private String cassandraKeyspace;

    protected Cluster cluster;
    protected Keyspace keyspace;
    
//    protected OutputCollector collector;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
//        LOG.debug("Preparing...");
        this.cassandraHost = (String) stormConf.get(CASSANDRA_HOST);
        this.cassandraKeyspace = (String) stormConf.get(CASSANDRA_KEYSPACE);
        this.cassandraPort = String.valueOf(stormConf.get(CASSANDRA_PORT));
        initCassandraConnection();
        
//        this.collector = collector;
    }

    private void initCassandraConnection() {
        try {
            this.cluster = HFactory.getOrCreateCluster("cassandra-bolt",
                            new CassandraHostConfigurator(this.cassandraHost
                                            + ":" + this.cassandraPort));
            this.keyspace = HFactory.createKeyspace(this.cassandraKeyspace,
                            this.cluster);
        }
        catch (Throwable e) {
            LOG.warn("Preparation failed.", e);
            throw new IllegalStateException("Failed to prepare CassandraBolt",
                            e);
        }
    }

}
