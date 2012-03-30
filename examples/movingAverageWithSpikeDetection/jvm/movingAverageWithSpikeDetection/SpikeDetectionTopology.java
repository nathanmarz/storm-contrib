package movingAverage;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class SpikeDetectionTopology {
    
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("string", new LightEventSpout(), 2);        
        builder.setBolt("movingAverage", new MovingAverageBolt(10), 2)
                .shuffleGrouping("string");
        builder.setBolt("spikes", new SpikeDetectionBolt(0.10f), 2)
        .shuffleGrouping("movingAverage");
        
        Config conf = new Config();
//        conf.setDebug(true);
       
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("spike", conf, builder.createTopology());
            Utils.sleep(600000);
            cluster.killTopology("spike");    
        }
    }
}