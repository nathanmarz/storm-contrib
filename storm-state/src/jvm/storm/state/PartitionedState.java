package storm.state;

import backtype.storm.task.TopologyContext;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONValue;

public class PartitionedState {
    public static State getState(Map conf, TopologyContext context, IPartitionedBackingStore store, StateFactory factory, Serializations sers) {
        int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        String metajson = store.getMeta();
        if(metajson!=null) {
            Map meta = (Map) JSONValue.parse(metajson);
            int numPartitions = ((Number) meta.get("numPartitions")).intValue();
            if(numPartitions!=numTasks) {
                throw new RuntimeException("Reading from partitioned meta with a different number of tasks than before. Should either adjust number of tasks or repartition the state");
            } 
        } else if(context.getThisTaskIndex()==0) {
            Map meta = new HashMap();
            meta.put("numPartitions", numTasks);
            store.storeMeta(JSONValue.toJSONString(meta));
        }
        IBackingStore backingStore = store.getBackingStore(context.getThisTaskIndex());
        backingStore.setExecutor(context.getSharedExecutor());
        State state = factory.makeState(conf, backingStore, sers);
        return state;
    }    
}
