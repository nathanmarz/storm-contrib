package storm.state.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IComponent;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import storm.state.State;

/**
 * These bolts will auto-commit / ack every topology.state.commit.freq.secs (defaults to 5 seconds)
 * Can have tuples be auto-acked via topology.state.immediate.ack (defaults to false). Otherwise, it acks after commit.
 * @author nathan
 */
public interface IStatefulBolt extends IComponent, IStateful {
    public static final String TOPOLOGY_STATE_COMMIT_FREQ_SECS = "topology.state.commit.freq.secs";
    public static final String TOPOLOGY_STATE_IMMEDIATE_ACK = "topology.state.immediate.ack";
    
    void prepare(Map conf, TopologyContext context, State state);
    void execute(Tuple tuple, BasicOutputCollector collector);
    void cleanup();
}
