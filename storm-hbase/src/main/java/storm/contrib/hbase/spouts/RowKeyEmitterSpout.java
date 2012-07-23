package storm.contrib.hbase.spouts;

import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.Map;
import java.util.UUID;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Random;
import org.apache.log4j.Logger;

/*
 * Spout emitting tuples containing the rowkey of the hbase table
 */
public class RowKeyEmitterSpout implements IRichSpout {
    
	private static final long serialVersionUID = 6814162766489261607L;
	public static Logger LOG = Logger.getLogger(RowKeyEmitterSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public RowKeyEmitterSpout() {
        this(true);
    }

    public RowKeyEmitterSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
    
    public boolean isDistributed() {
        return _isDistributed;
    }
    
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
    
    public void close() {
    }
        
    public void nextTuple() {
        Utils.sleep(100);
        Thread.yield();
        final String[] words = new String[] {"rowKey1", "rowKey2", "rowKey3", "rowKey4"};
        final Random rand = new Random();
        final String word = words[rand.nextInt(words.length)];
        _collector.emit(new Values(word), UUID.randomUUID());
    }
    
    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

	public void activate() {
	}

	public void deactivate() {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
