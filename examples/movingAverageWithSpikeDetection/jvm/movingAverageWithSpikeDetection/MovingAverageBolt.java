package movingAverage;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MovingAverageBolt implements IBasicBolt {
	private static final long serialVersionUID = 1L;
	private int movingAverageWindow = 1000;
	private Map<String, LinkedList<Double>> deviceIDtoStreamMap = new HashMap<String, LinkedList<Double>>();
	private Map<String, Double> deviceIDtoSumOfEvents = new HashMap<String, Double>();
	
	public MovingAverageBolt() {
		
	}
	
	public MovingAverageBolt(int movingAverageWindow) {
		this.movingAverageWindow = movingAverageWindow;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context) {
	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		final String deviceID = tuple.getString(0);
		final double nextDouble = (double)tuple.getInteger(1);
		double movingAvergeInstant = movingAverage(deviceID, nextDouble);
		System.out.println(movingAvergeInstant + " : " + nextDouble);
		collector.emit(new Values(deviceID, movingAvergeInstant, nextDouble));
	}
	
	public double movingAverage(String deviceID, double nextDouble) {
		LinkedList<Double> valueList = new LinkedList<Double>();
		double sum = 0.0;
		if (deviceIDtoStreamMap.containsKey(deviceID)) {
			valueList = deviceIDtoStreamMap.get(deviceID);
			sum = deviceIDtoSumOfEvents.get(deviceID);
			if (valueList.size() > movingAverageWindow-1) {
				double valueToRemove = valueList.removeFirst();			
				sum -= valueToRemove;
			}
			valueList.addLast(nextDouble);
			sum += nextDouble;
			deviceIDtoSumOfEvents.put(deviceID, sum);
			deviceIDtoStreamMap.put(deviceID, valueList);
			return sum/valueList.size();
		}
		else {
			valueList.add(nextDouble);
			deviceIDtoStreamMap.put(deviceID, valueList);
			deviceIDtoSumOfEvents.put(deviceID, nextDouble);
			return nextDouble;
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("string", "double", "double"));
	}
}