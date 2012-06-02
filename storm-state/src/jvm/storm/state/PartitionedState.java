package storm.state;

import backtype.storm.task.TopologyContext;
import java.util.Map;
import storm.state.hdfs.HDFSUtils;

public class PartitionedState {
    public static String thisStateDir(Map conf, TopologyContext context, String stateDir) {
        int numTasks = context.getComponentTasks(context.getThisComponentId()).size(); 
        // write metadata if not there already (with #partitions)
        // if #partitions != numtasks throw error
        return HDFSUtils.normalizePath(stateDir) + "/" + context.getThisTaskId();
    }
    
}
