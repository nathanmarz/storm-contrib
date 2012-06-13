package storm.state.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IComponent;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import storm.state.State;

/**
 * These bolts automatically commit the state every topology.state.commit.freq.secs (defaults to 1 second). Tuples
 * are not acked until after the commit happens, so tuples will be appropriately replayed.
 * 
 * Can have tuples be acked immediately (without waiting for commit) via
 * topology.state.immediate.ack config (defaults to false). Otherwise, it acks after commit.
 */
public interface IStatefulBolt extends IComponent, IStateful {
    public static final String TOPOLOGY_STATE_COMMIT_FREQ_SECS = "topology.state.commit.freq.secs";
    public static final String TOPOLOGY_STATE_IMMEDIATE_ACK = "topology.state.immediate.ack";
    
    void prepare(Map conf, TopologyContext context, State state);
    /**
     * Called for each tuple received. Should update the state here.
     */
    void execute(Tuple tuple, BasicOutputCollector collector);

    /**
     * Called before the commit happens. This is useful for emitting events on the same time interval as the commit.
     */
    void preCommit(BasicOutputCollector collector);
    void cleanup();
}
