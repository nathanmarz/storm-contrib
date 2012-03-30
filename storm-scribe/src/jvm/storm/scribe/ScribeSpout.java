package storm.scribe;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class ScribeSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    LinkedBlockingQueue<byte[]> _events;
    Scheme _scheme;
    ScribeReceiver _receiver;
    String _zkStr;
    String _zkRoot;
    
    public ScribeSpout(String zkStr, String zkRoot, Scheme scheme) {
        _zkStr = zkStr;
        _zkRoot = zkRoot;
        _scheme = scheme;
    }

    public ScribeSpout(String zkStr, String zkRoot) {
        this(zkStr, zkRoot, new RawScheme());
    }    
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _events = ScribeReceiver.makeEventsQueue(conf);
        _receiver = new ScribeReceiver(_events, conf, context, _zkStr, _zkRoot);
    }
    
    @Override
    public void nextTuple() {
        byte[] o = _events.poll();
        if(o!=null) {
            _collector.emit(new Values(_scheme.deserialize(o)));
        }
    }

    @Override
    public void activate() {
        _receiver.activate();
    }

    @Override
    public void deactivate() {
        _receiver.deactivate();
        
        // flush buffer on deactivation
        // this might not respect topology-max-spout-pending, but it's the best we can do
        while(!_events.isEmpty()) {
            nextTuple();
        }
    }

    @Override
    public void close() {
        _receiver.shutdown();
    }    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
    }
}
