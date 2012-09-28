package storm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.List;

public class TestZooKeeperDiscoveryTopology {
    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple.toString());
        }
    }

    public static void main(String [] args) throws Exception {
        List<String> zkDiscoveryServers = new ArrayList<String>();
        zkDiscoveryServers.add("10.100.1.160:2181");
        zkDiscoveryServers.add("10.100.1.161"); // can skip the port, defaults to 2181
        zkDiscoveryServers.add("10.100.1.162");

        SpoutConfig kafkaConf = new SpoutConfig(null, 1, "kafkastorm-zoo", "/kafkastorm-zoo", "kafkastorm-zoo");
        kafkaConf.scheme = new StringScheme();
        kafkaConf.hosts = ZooKeeperDiscovery.loadZkBrokers( zkDiscoveryServers, 20000, "/brokers/ids" );

        LocalCluster cluster = new LocalCluster();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka", new KafkaSpout(kafkaConf), 3);
        builder.setBolt("printer", new PrinterBolt())
                .shuffleGrouping("kafka");
        cluster.submitTopology("kafka-test", new Config(), builder.createTopology());
        Thread.sleep(600000);
    }
}
