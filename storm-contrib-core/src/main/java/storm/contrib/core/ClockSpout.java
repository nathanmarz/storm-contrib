package storm.contrib.core;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.utils.Utils;

/**
 * A generic template for a Spout that emits a tuple at a regular interval (in
 * terms of real time).
 * 
 * Due to the time-sensitive nature of this task, there is no point in making
 * these ticks reliable.
 * 
 * @author Adrian Petrescu <apetresc@gmail.com>
 *
 */
public abstract class ClockSpout implements IRichSpout {
	private SpoutOutputCollector collector;
	
    private boolean activated = false;
	protected final String streamId;
    private long i;
    private long nextTupleMillis = 0;

	/**
	 * @param streamId The stream on which to emit
	 */
	protected ClockSpout(String streamId) {
		this.streamId = streamId;
	}

	@Override
    public void activate() {
        activated = true;
    }

    @Override
    public void deactivate() {
        activated = false;
    }

    @Override
	public void open(
			@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		this.collector = collector;
		i = 0;

        this.activate();
	}

	@Override
	public void close() { }

	@Override
	public void nextTuple() {
        if (activated) {

            if (System.currentTimeMillis() > nextTupleMillis) {
		collector.emit(streamId, getTupleForTick(i));
                nextTupleMillis = System.currentTimeMillis() + getDelayForTick(i);
		
		i++;
	}
        }
    }
	
	/**
	 * Returns the data to be included in the {@code i}-th tick. Subclasses
	 * should ensure that they are also overriding 
	 * {@link #declareOutputFields(OutputFieldsDeclarer) declareOutputFields}
	 * to match the structure being returned here.
	 *
	 * @param i the number of ticks that have already been emitted by this
	 * spout
	 * @return the data to be included in the {@code i}-th tick
	 */
    public abstract List<Object> getTupleForTick(long i);
	
	/**
	 * Returns the delay between the {@code i} and {@code i+1}-th ticks, in
	 * milliseconds.
	 *
	 * @param i the number of ticks that have already been emitted by this
	 * spout
	 * @return the delay between the {@code i} and {@code i+1}-th ticks
	 */
    public abstract long getDelayForTick(long i);

	/**
	 * Marked final to emphasize to subclasses that there is no point
	 * in implementing this (emitted tuples are not anchored).
	 */
	@Override
	public final void ack(Object msgId) { }

	/**
	 * Marked final to emphasize to subclasses that there is no point
	 * in implementing this (emitted tuples are not anchored).
	 */
	@Override
	public final void fail(Object msgId) { }
}
