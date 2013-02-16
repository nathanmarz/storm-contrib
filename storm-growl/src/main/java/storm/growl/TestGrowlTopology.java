package storm.growl;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestGrowlTopology {
	public static void main(String[] args) throws Exception {
		
		GrowlConfig growlConf = new GrowlConfig("localhost");
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new GrowlTestSpout(), 1);
		
		builder.setBolt("GrowlBolt", new GrowlBolt(growlConf), 1)
				.allGrouping("spout");
		
		/* sticky growl */
		growlConf.sticky = true;
		builder.setBolt("StickyBolt", new GrowlBolt(growlConf), 1)
				.allGrouping("GrowlBolt");
		
		Config conf = new Config();
        conf.setDebug(true);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        
        } else {
        	/* local mode */
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }		
		
	}
	
	
	/*
	 * test spout for GrowlBolt
	 */
	public static class GrowlTestSpout extends BaseRichSpout{
		
		SpoutOutputCollector _collector;
		
		@SuppressWarnings("rawtypes")
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void nextTuple() {
			String title = "Storm Growl";
			String message = "Hello Growl!";
			
			_collector.emit(new Values(title, message));
	        Utils.sleep(5000);
		}
		
		@Override
		public Map<String, Object> getComponentConfiguration() {
			Config conf = new Config();			
			return conf;
		}
		
		@Override
		public void ack(Object msgId) {
		}

		@Override
		public void fail(Object msgId) {
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("title", "message"));
		}
		
	}
}
