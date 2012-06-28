package storm.contrib.core;

import java.util.List;
import java.util.Map;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * A simple implementation of {@link ClockSpout} which emits a barebones tick
 * tuple at a fixed rate.
 *
 * Every {@code delay} milliseconds, this spout will emit a tuple containing a
 * single field, {@code tick}, specifying how many tuples have already been
 * emitted.
 *
 * @author Adrian Petrescu <apetresc@gmail.com>
 *
 */
public class SimpleClockSpout extends ClockSpout {
	private final int delay;

	/**
	 * @param streamId The stream on which to emit
	 * @param delay The fixed amount of time (in milliseconds) between ticks
	 */
	public SimpleClockSpout(String streamId, int delay) {
		super(streamId);
		this.delay = delay;
	}

	@Override
    public List<Object> getTupleForTick(long i) {
		return Utils.tuple(i);
	}

	@Override
    public long getDelayForTick(long i) {
		return delay;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tick"));
		declarer.declareStream(streamId, new Fields("tick"));
	}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        backtype.storm.Config conf = new backtype.storm.Config();

        return conf;
    }
}
