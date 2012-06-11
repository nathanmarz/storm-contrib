package storm.state.example;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.Random;
import storm.state.MapState;
import storm.state.Serializations;
import storm.state.State;
import storm.state.StateFactory;
import storm.state.bolt.IStatefulBolt;
import storm.state.bolt.StatefulBoltExecutor;

public class MapExample {
    
    public static class ThrottledWordSpout extends BaseRichSpout {
        public static final String[] VALS = new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
        
        private Random _rand;
        private int _perSecond;
        private int _size;
        private long _lastCheckpoint;
        private int _emittedSinceCheckpoint = 0;
        SpoutOutputCollector _collector;
        
        
        public ThrottledWordSpout(int size, int perSecond) {
            _size = size;
            _perSecond = perSecond;            
        }
        
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _rand = new Random();
            _lastCheckpoint = System.currentTimeMillis();
            _collector = collector;
        }

        private String genValue() {
            StringBuilder sb = new StringBuilder();
            for(int i=0; i<_size; i++) {
                sb.append(VALS[_rand.nextInt(VALS.length)]);
            }
            return sb.toString();
        }
        
        @Override
        public void nextTuple() {
           _emittedSinceCheckpoint++;
           _collector.emit(new Values(genValue()));
           if(_emittedSinceCheckpoint >= _perSecond) {
               long now = System.currentTimeMillis();
               long delta = _lastCheckpoint + 1000 - now;
               if(delta > 0) {
                   Utils.sleep(delta);
               }
               _emittedSinceCheckpoint = 0;
               _lastCheckpoint = now;
           }
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }
    
    public static class WordCount implements IStatefulBolt {
        MapState _state;
        Map<String, Integer> _pending;
        
        @Override
        public void prepare(Map conf, TopologyContext context, State state) {
            _state = (MapState) state;
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            if(_pending.containsKey(word)) {
                _pending.put(word, _pending.get(word) + 1);                
            } else {
                _pending.put(word, 1);            
            }
        }

        @Override
        public void preCommit(BasicOutputCollector collector) {
            for(String w: _pending.keySet()) {
                int inc = _pending.get(w);
                Integer amt = (Integer) _state.get(w);
                if(amt==null) amt = inc;
                else amt = amt + inc;
                _state.put(w, amt);
            }
            System.out.println(_state.size());
        }

        @Override
        public void cleanup() {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        @Override
        public StateFactory getStateBuilder() {
            return new MapState.Factory();
        }

        @Override
        public Serializations getSerializations() {
            return new Serializations();
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new ThrottledWordSpout(6, 1000), 4);
        builder.setBolt("counter", new StatefulBoltExecutor(new WordCount(), "hdfs://10.202.7.99:8020/tmp/data"), 8)
                .fieldsGrouping("spout", new Fields("word"));
        
        Config conf = new Config();
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    
}
