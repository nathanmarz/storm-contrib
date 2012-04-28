// Copyright (c) 2012 P. Taylor Goetz

package backtype.storm.contrib.cassandra.example;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.cassandra.bolt.CassandraConstants;
import backtype.storm.contrib.cassandra.bolt.DelimitedColumnLookupBolt;
import backtype.storm.contrib.cassandra.bolt.ValueLessColumnLookupBolt;
import backtype.storm.drpc.CoordinatedBolt.FinishedCallback;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CassandraReachTopology implements CassandraConstants{

    public static void main(String[] args) throws Exception{
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
        
//        DelimitedColumnLookupBolt tweetersBolt = 
//                        new DelimitedColumnLookupBolt("tweeters_delimited", "rowKey", "tweeted_by", ":", "rowKey", "tweeter", true);
//        
//        DelimitedColumnLookupBolt followersBolt = 
//                        new DelimitedColumnLookupBolt("followers_delimited", "tweeter", "followers", ":", "rowKey", "follower", true);
        
        ValueLessColumnLookupBolt tweetersBolt = 
                        new ValueLessColumnLookupBolt("tweeters", "rowKey","rowKey", "tweeter", true);
        
        ValueLessColumnLookupBolt followersBolt = 
                        new ValueLessColumnLookupBolt("followers", "tweeter", "rowKey", "follower", true);
        
        builder.addBolt(new InitBolt());
        builder.addBolt(tweetersBolt).shuffleGrouping();
        builder.addBolt(followersBolt).shuffleGrouping();
        builder.addBolt(new PartialUniquer()).fieldsGrouping(new Fields("id", "follower"));
        builder.addBolt(new CountAggregator()).fieldsGrouping(new Fields("id"));
        
        
        Config config = new Config();
        config.put(CASSANDRA_HOST, "localhost");
        config.put(CASSANDRA_PORT, 9160);
        config.put(CASSANDRA_KEYSPACE, "stormks");
        
        if(args==null || args.length==0) {
            config.setMaxTaskParallelism(3);
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("reach-drpc", config, builder.createLocalTopology(drpc));
            
            String[] urlsToTry = new String[] {"http://github.com/hmsonline","http://github.com/nathanmarz", "http://github.com/ptgoetz", "http://github.com/boneill"};
            for(String url: urlsToTry) {
                System.out.println("Reach of " + url + ": " + drpc.execute("reach", url));
            }
            
            cluster.shutdown();
            drpc.shutdown();
        } else {
            config.setNumWorkers(6);
            StormSubmitter.submitTopology(args[0], config, builder.createRemoteTopology());
        }
    }
    
    public static class InitBolt implements IBasicBolt {

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "rowKey")); 
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            collector.emit(input.getValues());
        }

        @Override
        public void cleanup() {

        }
        
    }
    
    public static class PartialUniquer implements IRichBolt, FinishedCallback {
        OutputCollector collector;
        Map<Object, Set<String>> sets = new HashMap<Object, Set<String>>();
        
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            Object id = tuple.getValue(0);
            Set<String> curr = this.sets.get(id);
            if(curr==null) {
                curr = new HashSet<String>();
                this.sets.put(id, curr);
            }
            curr.add(tuple.getString(2));
            collector.ack(tuple);
        }

        @Override
        public void cleanup() {
        }

        @Override
        public void finishedId(Object id) {
            Set<String> curr = this.sets.remove(id);
            int count;
            if(curr!=null) {
                count = curr.size();
            } else {
                count = 0;
            }
            collector.emit(new Values(id, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "partial-count"));
        }
    }
    
    public static class CountAggregator implements IRichBolt, FinishedCallback {
        Map<Object, Integer> counts = new HashMap<Object, Integer>();
        OutputCollector collector;
        
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            Object id = tuple.getValue(0);
            int partial = tuple.getInteger(1);
            
            Integer curr = counts.get(id);
            if(curr==null) curr = 0;
            this.counts.put(id, curr + partial);
            this.collector.ack(tuple);
        }

        @Override
        public void cleanup() {
        }

        @Override
        public void finishedId(Object id) {
            Integer reach = counts.get(id);
            if(reach==null) reach = 0;
            collector.emit(new Values(id, reach));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "reach"));
        }
        
    }
    
}
