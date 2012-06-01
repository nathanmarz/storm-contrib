package storm.contrib.rdbms;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Class implementing a test spout emitting a stream of tuples, each having a String and an integer
 */
public class SampleSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	boolean _isDistributed;
	Random _rand;  
	int count = 0;

	public SampleSpout() {
		this(true);
	}

	public SampleSpout(boolean isDistributed) {
		_isDistributed = isDistributed;
	}

	public boolean isDistributed() {
		return true;
	}

	@Override
	public void nextTuple() {
		Utils.sleep(1000);
		String[] words = new String[] {"indore", "india", "impetus", "tiwari", "jayati"};
		Integer[] numbers = new Integer[] {
				11, 22, 33, 44, 55
		};

		if(count == numbers.length -1) {
			count = 0;
		}
		count ++;
		int number = numbers[count];
		String word = words[count];
		_collector.emit(new Values(word, number));

	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	@Override
	public void close() {        
	}

	@Override
	public void ack(Object id) {
	}
 
	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "number"));
	}

	
	public void activate() {
		// TODO Auto-generated method stub

	}

	
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}




