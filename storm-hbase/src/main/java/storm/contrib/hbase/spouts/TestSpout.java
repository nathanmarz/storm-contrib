package storm.contrib.hbase.spouts;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestSpout implements IRichSpout {

	SpoutOutputCollector _collector;
	Random _rand;  
	int count = 0;


	public boolean isDistributed() {
		return true;
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	public void nextTuple() {
		Utils.sleep(1000);
		String[] words = new String[] { "hello", "tiwari", "indore", "jayati"};
		Integer[] numbers = new Integer[] {
				1,2,3,4,5
		};

		if(count == numbers.length -1) {
			count = 0;
		}
		count ++;
		int number = numbers[count];
		String word = words[count];
		int randomNum = (int) (Math.random()*1000);
		_collector.emit(new Values(word, number));
	}


	public void close() {        
	}


	public void ack(Object id) {
	}

	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "number"));
	}

	public void activate() {
	}

	public void deactivate() {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}




