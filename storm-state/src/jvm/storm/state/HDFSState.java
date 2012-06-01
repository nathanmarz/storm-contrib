package storm.state;

import carbonite.JavaBridge;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.BigIntegerSerializer;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.log4j.Logger;
import storm.state.HDFSLog.LogWriter;


public class HDFSState {
    public static final Logger LOG = Logger.getLogger(HDFSState.class);

    public static interface State {
        public void setState(Object snapshot);
    }
    
    List<Transaction> _pendingTransactions = new ArrayList<Transaction>();
    Kryo _serializer;
    BigInteger _currVersion;
    
    FileSystem _fs;
    String _rootDir;
    
    LogWriter _openLog;
    boolean _isLocal = false;
        
    public HDFSState(String dfsDir, Serializations sers) {
        try {
            _fs = new Path(dfsDir).getFileSystem(new Configuration());
            if(_fs instanceof LocalFileSystem) {
                LOG.info("Using local filesystem and disabling checksums");
                _fs = new RawLocalFileSystem();
                _isLocal = true;
                try {
                    ((RawLocalFileSystem) _fs).initialize(new URI("file://localhost/"), new Configuration());
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
            _rootDir = new Path(dfsDir).toString();
            HDFSUtils.mkdirs(_fs, tmpDir());
            HDFSUtils.mkdirs(_fs, snapshotDir());
            HDFSUtils.mkdirs(_fs, logDir());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        _serializer = new Kryo();
        sers.apply(_serializer);
        _serializer.register(Commit.class);
        _serializer.register(Snapshot.class);
        _serializer.register(BigInteger.class, new BigIntegerSerializer());
        try {
            // automatically support clojure collections since those are useful
            // for implementing these kinds of structures
            JavaBridge.registerCollections(_serializer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        cleanup();
    }
    
    public Object appendAndApply(Transaction entry, State state) {
        _pendingTransactions.add(entry);
        return entry.apply(state);
    }
    
    public BigInteger getVersion() {
        return _currVersion;
    }
    
    public void commit() {
        commit(null);
    }
    
    public void commit(BigInteger txid) {
        Commit commit = new Commit(txid, _pendingTransactions);
        _pendingTransactions = new ArrayList<Transaction>();
        _openLog.write(commit);
        if(_isLocal) {
            // see HADOOP-7844
            rotateLog();        
        } else {
            _openLog.sync();            
        }
        _currVersion = txid;
    }
    
    public void resetToLatest(State state) {
       // TODO: probably much better to serialize the snapshot directly into the output stream to prevent
        // so much more memory usage
        Long latestSnapshot = latestSnapshot();
        BigInteger version = BigInteger.ZERO;
        if(latestSnapshot==null) {
            state.setState(null);
        } else {
            Snapshot snapshot = readSnapshot(_fs, latestSnapshot);
            state.setState(snapshot.snapshot);
            version = snapshot.txid;
        }
        if(latestSnapshot==null) latestSnapshot = -1L;
        List<Long> txLogs = allTxlogs();
        for(Long l: txLogs) {
            if(l > latestSnapshot) {
                HDFSLog.LogReader r = HDFSLog.open(_fs, txlogPath(l), _serializer);
                while(true) {
                    Commit c = (Commit) r.read();
                    if(c==null) break;
                    version = c.txid;
                    for(Transaction t: c.transactions) {
                        t.apply(state);
                    }
                }
            }
        }
        _currVersion = version;
        rotateLog();
    }
    
    private long latestSnapshotOrLogVersion() {
        Long latestSnapshot = latestSnapshot();
        Long latestTxlog = latestTxlog();
        long newTxLogVersion;
        if(latestSnapshot==null) latestSnapshot = -1L;
        if(latestTxlog==null) latestTxlog = -1L;
        return Math.max(latestSnapshot, latestTxlog);
    }
    
    private void rotateLog() {
        if(_openLog!=null) {
            _openLog.close();
        }
        long newTxLogVersion = latestSnapshotOrLogVersion() + 1;
        _openLog = HDFSLog.create(_fs, txlogPath(newTxLogVersion), _serializer);        
        
    }

    
    //TODO: need to do this in the background... need an executor threadpool that can be used
    public void compact(Object immutableSnapshot) {
        Snapshot toWrite = new Snapshot(_currVersion, immutableSnapshot);
        long snapVersion = latestTxlog();
        if(snapVersion<0) {
            throw new RuntimeException("Expected a txlog version greater than or equal to 0");
        }
        rotateLog();
        writeSnapshot(_fs, snapVersion, toWrite);
        cleanup();
    }
    
    public static class Snapshot {
        BigInteger txid;
        Object snapshot;
        
        //for kryo
        public Snapshot() {
            
        }
        
        public Snapshot(BigInteger txid, Object snapshot) {
            this.txid = txid;
            this.snapshot = snapshot;
        }
    }
    
    public static class Commit {
        BigInteger txid;
        List<Transaction> transactions;
        
        //for kryo
        public Commit() {
            
        }
        
        public Commit(BigInteger txid, List<Transaction> transactions) {
            this.txid = txid;
            this.transactions = transactions;
        }        
    }
    
    private String tmpDir() {
        return _rootDir + "/tmp";
    }
    
    private String snapshotDir() {
        return _rootDir + "/snapshots";
    }
    
    private String logDir() {
        return _rootDir + "/txlog";
    }
    
    private String snapshotPath(Long version) {
        return snapshotDir() + "/" + version + ".snapshot";
    }
    
    private String txlogPath(Long version) {
        return logDir() + "/" + version + ".txlog";
    }    
    
    private List<Long> allSnaphots() {
        return HDFSUtils.getSortedVersions(_fs, snapshotDir(), ".snapshot");
    }

    private List<Long> allTxlogs() {
        return HDFSUtils.getSortedVersions(_fs, logDir(), ".txlog");
    }    
    
    private Long latestSnapshot() {
        List<Long> all = allSnaphots();
        if(all.isEmpty()) return null;
        else return all.get(all.size()-1);
    }

    private Long latestTxlog() {
        List<Long> all = allTxlogs();
        if(all.isEmpty()) return null;
        else return all.get(all.size()-1);
    }    
    
    private void cleanup() {
        Long latest = latestSnapshot();
        HDFSUtils.clearDir(_fs, tmpDir());
        if(latest!=null) {
            for(Long s: allSnaphots()) {
                if(s < latest) {
                    HDFSUtils.deleteFile(_fs, snapshotPath(s));
                }
            }
            for(Long t: allTxlogs()) {
                if(t <= latest) {
                    HDFSUtils.deleteFile(_fs, txlogPath(t));
                }
            }
        }        
    }
    
    private Snapshot readSnapshot(FileSystem fs, long version) {
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(snapshotPath(version)));
            Input input = new Input(in);
            Snapshot ret = _serializer.readObject(input, Snapshot.class);
            input.close();
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void writeSnapshot(FileSystem fs, long version, Snapshot snapshot) {
        try {
            String finalPath = snapshotPath(version);
            String tmpPath = tmpDir() + "/" + version + ".tmp";
            FSDataOutputStream os = fs.create(new Path(tmpPath));
            Output output = new Output(os);
            _serializer.writeObject(output, snapshot);
            output.flush();
            output.close();
            fs.rename(new Path(tmpPath), new Path(finalPath));
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
