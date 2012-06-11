package storm.state.hdfs;

import carbonite.JavaBridge;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.BigIntegerSerializer;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.log4j.Logger;
import storm.state.hdfs.HDFSLog.LogWriter;
import storm.state.Serializations;
import storm.state.State;
import storm.state.Transaction;


public class HDFSState {
    public static final Logger LOG = Logger.getLogger(HDFSState.class);
    public static final String AUTO_COMPACT_BYTES_CONFIG = "topology.state.auto.compact.bytes";
        
    List<Transaction> _pendingTransactions = new ArrayList<Transaction>();
    Kryo _serializer;
    BigInteger _currVersion;
    
    FileSystem _fs;
    String _rootDir;
    
    LogWriter _openLog;
    boolean _isLocal = false;
    Semaphore _compactionWaiter = new Semaphore(1);
    long _writtenSinceCompaction = 0;
    long _autoCompactFrequencyBytes;
        
    public HDFSState(Map conf, String dfsDir, Serializations sers) {
        _fs = HDFSUtils.getFS(dfsDir);
        _isLocal = _fs instanceof RawLocalFileSystem;
        _rootDir = new Path(dfsDir).toString();
        
        Number autoCompactFrequency = (Number) conf.get(AUTO_COMPACT_BYTES_CONFIG);
        if(autoCompactFrequency == null) autoCompactFrequency = 1024 * 1024;
        _autoCompactFrequencyBytes = autoCompactFrequency.longValue();
        
        HDFSUtils.mkdirs(_fs, tmpDir());
        HDFSUtils.mkdirs(_fs, snapshotDir());
        HDFSUtils.mkdirs(_fs, logDir());
        
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
    
    Executor _executor = null;
    
    public void setExecutor(Executor executor) {
        _executor = executor;
    }
    
    public Object appendAndApply(Transaction entry, State state) {
        _pendingTransactions.add(entry);
        return entry.apply(state);
    }
    
    public BigInteger getVersion() {
        return _currVersion;
    }
    
    public void commit(State state) {
        commit(null, state);
    }
    
    public void commit(BigInteger txid, State state) {
        Commit commit = new Commit(txid, _pendingTransactions);
        _pendingTransactions = new ArrayList<Transaction>();
        if(txid==null || txid.equals(getVersion())) {
            _writtenSinceCompaction += _openLog.write(commit);
            if(_isLocal) {
                // see HADOOP-7844
                rotateLog();        
            } else {
                _openLog.sync();            
            }
            _currVersion = txid;
            if(_autoCompactFrequencyBytes > 0 && _writtenSinceCompaction > _autoCompactFrequencyBytes) {
                compactAsync(state);
                _writtenSinceCompaction = 0;
            }
        } else {
            // we've done this update before, so reset the state
            resetToLatest(state);
        }
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
        _writtenSinceCompaction = 0;
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
                _writtenSinceCompaction += r.amtRead();
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
    
    public void compact(State state) {
        Object snapshot = state.getSnapshot();
        long version = prepareCompact(snapshot);
        doCompact(version, new Snapshot(_currVersion, snapshot));        
    }
    
    public void compactAsync(State state) {
        Object immutableSnapshot = state.getSnapshot();
        if(_executor==null) {
            throw new RuntimeException("Need to configure with an executor to run compactions in the background");
        }
        final long version = prepareCompact(immutableSnapshot);
        final Snapshot snapshot = new Snapshot(_currVersion, immutableSnapshot);
        _executor.execute(new Runnable() {
            @Override
            public void run() {
                doCompact(version, snapshot);
            }            
        });
    }
    
    private long prepareCompact(Object immutableSnapshot) {
        //TODO: maybe it's better to skip the compaction if it's currently going on (rather than block here)
        try {
            _compactionWaiter.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        long snapVersion = latestTxlog();
        if(snapVersion<0) {
            throw new RuntimeException("Expected a txlog version greater than or equal to 0");
        }
        rotateLog();
        return snapVersion;
    }

    private void doCompact(long version, Snapshot snapshot) {
        try {
            writeSnapshot(_fs, version, snapshot);
            cleanup();
        } finally {
            _compactionWaiter.release();
        }
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
    
    public void close() {
        _openLog.close();
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
