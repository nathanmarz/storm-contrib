package storm.state.bolt;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import storm.state.State;

/**
 * execute is used to figure out the partial aggregation that will be added to the state
 * 
 * updateState is called if the transaction id of the state is different than the transaction id of this bolt
 *  - this does not support opaque transactional topologies yet – for that, a state would need to be able to rollback to a previous version
 *  - which is doable if snapshot is actually state before last commit + the commit
 * 
 * finishBatch is called once all state updates are complete. then this bolt can emit more tuples into the topology
 */
public interface IStatefulTransactionalBolt<T extends State> extends IComponent, IStateful {
    void prepare(Map conf, TopologyContext context, TransactionAttempt attempt);
    void execute(Tuple tuple);
    void updateState(T state);
    void finishBatch(T state, BatchOutputCollector collector);    
}
