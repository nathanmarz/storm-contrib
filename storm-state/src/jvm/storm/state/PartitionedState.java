package storm.state;

import backtype.storm.task.TopologyContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONValue;
import storm.state.hdfs.HDFSUtils;

public class PartitionedState {
    public static State getState(Map conf, TopologyContext context, String stateDir, StateFactory factory, Serializations sers) {
        try {
            int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
            stateDir = HDFSUtils.normalizePath(stateDir);
            String metaPath = stateDir + "/META";
            FileSystem fs = HDFSUtils.getFS(stateDir);
            if(fs.exists(new Path(metaPath))) {
                Map meta = readMeta(fs, metaPath);
                int numPartitions = ((Number) meta.get("numPartitions")).intValue();
                if(numPartitions!=numTasks) {
                    throw new RuntimeException("Reading from partitioned meta with a different number of tasks than before. Should either adjust number of tasks or repartition the state");
                }
            } else if(context.getThisTaskIndex()==0) {
                Map meta = new HashMap();
                meta.put("numPartitions", numTasks);
                writeMeta(fs, metaPath, meta);
            }
            String dir =  stateDir + "/" + context.getThisTaskId();
            State state = factory.makeState(conf, stateDir, sers);
            state.setExecutor(context.getSharedExecutor());
            return state;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }        
    }

    public static State getState(Map conf, TopologyContext context, String stateDir, StateFactory factory) {
        return getState(conf, context, stateDir, factory, new Serializations());
    }
        
    
    private static void writeMeta(FileSystem fs, String path, Map meta) throws IOException {
        String tmp = path + ".tmp";
        String toWrite = JSONValue.toJSONString(meta);
        FSDataOutputStream os = fs.create(new Path(tmp), true);
        os.writeUTF(toWrite);
        os.close();
        fs.rename(new Path(tmp), new Path(path));
    }
    
    private static Map readMeta(FileSystem fs, String path) throws IOException {
        FSDataInputStream is = fs.open(new Path(path));
        String json = is.readUTF();
        is.close();
        return (Map) JSONValue.parse(json);
    }
}
