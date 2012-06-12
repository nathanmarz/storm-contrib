package storm.state.hdfs;

import backtype.storm.serialization.types.ArrayListSerializer;
import backtype.storm.serialization.types.HashMapSerializer;
import backtype.storm.serialization.types.HashSetSerializer;
import backtype.storm.tuple.Values;
import carbonite.JavaBridge;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.BigIntegerSerializer;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
    public static final int DEFAULT_AUTO_COMPACT_BYTES = 10 * 1024 * 1024;
        
    List<Transaction> _pendingTransactions = new ArrayList<Transaction>();
    Kryo _fgSerializer;
    Kryo _bgSerializer;
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
        if(autoCompactFrequency == null) autoCompactFrequency = DEFAULT_AUTO_COMPACT_BYTES;
        _autoCompactFrequencyBytes = autoCompactFrequency.longValue();
        
        HDFSUtils.mkdirs(_fs, tmpDir());
        HDFSUtils.mkdirs(_fs, checkpointDir());
        HDFSUtils.mkdirs(_fs, logDir());
        
        _fgSerializer = makeKryo(sers);
        _bgSerializer = makeKryo(sers);
        cleanup();
    }
    
    public static Kryo makeKryo(Serializations sers) {
        Kryo ret = new Kryo();
        ret.setReferences(false);
        sers.apply(ret);
        ret.register(Commit.class);
        ret.register(Checkpoint.class);
        ret.register(BigInteger.class, new BigIntegerSerializer());
        ret.register(byte[].class);
        ret.register(ArrayList.class, new ArrayListSerializer(ret));
        ret.register(HashMap.class, new HashMapSerializer(ret));
        ret.register(HashSet.class, new HashSetSerializer(ret));
        ret.register(Values.class);
        
        try {
            // automatically support clojure collections since those are useful
            // for implementing these kinds of structures
            JavaBridge.registerCollections(ret);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
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
        // TODO: need to remember the snapshot since last commit.
        // "checkpoint" should be snapshot + last commit (makes it possible to 
        // roll back with transactional topologies
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
        Long latestCheckpoint = latestCheckpoint();
        BigInteger version = BigInteger.ZERO;
        if(latestCheckpoint==null) {
            state.setState(null);
        } else {
            Checkpoint checkpoint = readCheckpoint(_fs, latestCheckpoint, _fgSerializer);
            state.setState(checkpoint.snapshot);
            version = checkpoint.txid;
        }
        if(latestCheckpoint==null) latestCheckpoint = -1L;
        List<Long> txLogs = allTxlogs();
        _writtenSinceCompaction = 0;
        for(Long l: txLogs) {
            if(l > latestCheckpoint) {
                HDFSLog.LogReader r = HDFSLog.open(_fs, txlogPath(l), _fgSerializer);
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
    
    private long latestCheckpointOrLogVersion() {
        Long latestCheckpoint = latestCheckpoint();
        Long latestTxlog = latestTxlog();
        if(latestCheckpoint==null) latestCheckpoint = -1L;
        if(latestTxlog==null) latestTxlog = -1L;
        return Math.max(latestCheckpoint, latestTxlog);
    }
    
    private void rotateLog() {
        if(_openLog!=null) {
            _openLog.close();
        }
        long newTxLogVersion = latestCheckpointOrLogVersion() + 1;
        _openLog = HDFSLog.create(_fs, txlogPath(newTxLogVersion), _fgSerializer);        
        
    }
    
    public void compact(State state) {
        Object snapshot = state.getSnapshot();
        long version = prepareCompact();
        doCompact(version, new Checkpoint(_currVersion, snapshot));        
    }
    
    public void compactAsync(State state) {
        Object immutableSnapshot = state.getSnapshot();
        if(_executor==null) {
            throw new RuntimeException("Need to configure with an executor to run compactions in the background");
        }
        final long version = prepareCompact();
        final Checkpoint checkpoint = new Checkpoint(_currVersion, immutableSnapshot);
        _executor.execute(new Runnable() {
            @Override
            public void run() {
                doCompact(version, checkpoint);
            }            
        });
    }
    
    private long prepareCompact() {
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

    private void doCompact(long version, Checkpoint checkpoint) {
        try {
            writeCheckpoint(_fs, version, checkpoint, _bgSerializer);
            cleanup();
        } finally {
            _compactionWaiter.release();
        }
    }
    
    
    public static class Checkpoint {
        BigInteger txid;
        Object snapshot;
        
        //for kryo
        public Checkpoint() {
            
        }
        
        public Checkpoint(BigInteger txid, Object snapshot) {
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
    
    private String checkpointDir() {
        return _rootDir + "/checkpoints";
    }
    
    private String logDir() {
        return _rootDir + "/txlog";
    }
    
    private String checkpointPath(Long version) {
        return checkpointDir() + "/" + version + ".checkpoint";
    }
    
    private String txlogPath(Long version) {
        return logDir() + "/" + version + ".txlog";
    }    
    
    private List<Long> allCheckpoints() {
        return HDFSUtils.getSortedVersions(_fs, checkpointDir(), ".checkpoint");
    }

    private List<Long> allTxlogs() {
        return HDFSUtils.getSortedVersions(_fs, logDir(), ".txlog");
    }    
    
    private Long latestCheckpoint() {
        List<Long> all = allCheckpoints();
        if(all.isEmpty()) return null;
        else return all.get(all.size()-1);
    }

    private Long latestTxlog() {
        List<Long> all = allTxlogs();
        if(all.isEmpty()) return null;
        else return all.get(all.size()-1);
    }    
    
    private void cleanup() {
        Long latest = latestCheckpoint();
        HDFSUtils.clearDir(_fs, tmpDir());
        if(latest!=null) {
            for(Long s: allCheckpoints()) {
                if(s < latest) {
                    HDFSUtils.deleteFile(_fs, checkpointPath(s));
                }
            }
            for(Long t: allTxlogs()) {
                if(t <= latest) {
                    HDFSUtils.deleteFile(_fs, txlogPath(t));
                }
            }
        }        
    }
    
    private Checkpoint readCheckpoint(FileSystem fs, long version, Kryo kryo) {
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(checkpointPath(version)));
            Input input = new Input(in);
            Checkpoint ret = kryo.readObject(input, Checkpoint.class);
            input.close();
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void writeCheckpoint(FileSystem fs, long version, Checkpoint checkpoint, Kryo kryo) {
        try {
            String finalPath = checkpointPath(version);
            String tmpPath = tmpDir() + "/" + version + ".tmp";
            FSDataOutputStream os = fs.create(new Path(tmpPath), true);
            Output output = new Output(os);
            kryo.writeObject(output, checkpoint);
            output.flush();
            output.close();
            fs.rename(new Path(tmpPath), new Path(finalPath));
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
