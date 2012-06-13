package storm.state.hdfs;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import storm.state.IBackingStore;
import storm.state.IPartitionedBackingStore;


public class HDFSStore implements IPartitionedBackingStore {
    String _dir;
    FileSystem _fs;
    
    public HDFSStore(String dfsDir) {
        _dir = HDFSUtils.normalizePath(dfsDir);
    }

    @Override
    public void init() {
        _fs = HDFSUtils.getFS(_dir);
    }
    
    @Override
    public void storeMeta(String meta) {
        try {
            writeMeta(_fs, metaPath(), meta);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getMeta() {
        try {
            if(_fs.exists(new Path(metaPath()))) {
                return readMeta(_fs, metaPath());
            } else {
                return null;
            }
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IBackingStore getBackingStore(int partition) {
        return new HDFSBackingStore(_dir + "/" + partition);
    }
    
    private String metaPath() {
        return _dir + "/META";
    }    
    
    private static void writeMeta(FileSystem fs, String path, String toWrite) throws IOException {
        String tmp = path + ".tmp";
        FSDataOutputStream os = fs.create(new Path(tmp), true);
        os.writeUTF(toWrite);
        os.close();
        fs.rename(new Path(tmp), new Path(path));
    }
    
    private static String readMeta(FileSystem fs, String path) throws IOException {
        FSDataInputStream is = fs.open(new Path(path));
        String ret = is.readUTF();
        is.close();
        return ret;
    }    
}
