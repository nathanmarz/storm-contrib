package storm.scribe;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalSpout;
import backtype.storm.transactional.ITransactionalSpout.Coordinator;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

public class BatchScribeSpout extends BaseTransactionalSpout {
    public class ScribeCoordinator implements Coordinator {
        @Override
        public Object initializeTransaction(BigInteger txid, Object lastMeta) {
            return null;
        }

        @Override
        public void close() {
        }
        
        @Override
        public boolean isReady() {
            return true;
        }
    }
    
    public class ScribeEmitter implements Emitter {
        LinkedBlockingQueue<byte[]> _events;
        ScribeReceiver _receiver;
        List<byte[]> _buffer;
        
        public ScribeEmitter(Map conf, TopologyContext context) {
            _events = ScribeReceiver.makeEventsQueue(conf);
            _buffer = new ArrayList<byte[]>();
            _receiver = new ScribeReceiver(_events, conf, context, _zkStr, _zkRoot);
        }
        
        @Override
        public void emitBatch(TransactionAttempt tx, Object meta, BatchOutputCollector collector) {
            _buffer.clear();
            _events.drainTo(_buffer);
            for(byte[] elem: _buffer) {
                LinkedList<Object> values = new LinkedList<Object>(_scheme.deserialize(elem));
                values.addFirst(tx);
                collector.emit(values);
            }            
        }

        @Override
        public void cleanupBefore(BigInteger txid) {
        }

        @Override
        public void close() {
        }
        
    }
    
    String _zkStr;
    String _zkRoot;
    Scheme _scheme;
    
    public BatchScribeSpout(String zkStr, String zkRoot, Scheme scheme) {
        _zkStr = zkStr;
        _zkRoot = zkRoot;
        _scheme = scheme;
    }

    public BatchScribeSpout(String zkStr, String zkRoot) {
        this(zkStr, zkRoot, new RawScheme());
    }    
    
    @Override
    public Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new ScribeCoordinator();
    }

    @Override
    public Emitter getEmitter(Map conf, TopologyContext context) {
        return new ScribeEmitter(conf, context);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "event"));
    }    
}
