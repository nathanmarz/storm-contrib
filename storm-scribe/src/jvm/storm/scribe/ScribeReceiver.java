package storm.scribe;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import storm.scribe.generated.LogEntry;
import storm.scribe.generated.ResultCode;
import storm.scribe.generated.fb_status;
import storm.scribe.generated.scribe;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.server.TServer;
import org.apache.thrift7.TException;
import org.apache.thrift7.server.THsHaServer;
import org.apache.thrift7.transport.TNonblockingServerSocket;
import org.apache.thrift7.transport.TTransportException;
import org.apache.zookeeper.CreateMode;

public class ScribeReceiver {
    LinkedBlockingQueue<byte[]> _events;
    volatile boolean _active = false;
    volatile boolean _scribeActive = false;
    final Object _scribeLock = new Object();
    TServer _server;
    String _zkStr;
    String _zkRoot;
    CuratorFramework _zk;
    
    public static LinkedBlockingQueue<byte[]> makeEventsQueue(Map conf) {
        Number bufferSize = (Number) conf.get("scribe.spout.buffer.size");
        if(bufferSize==null) bufferSize = 10000;
        return new LinkedBlockingQueue<byte[]>(bufferSize.intValue());
    }
    
    public ScribeReceiver(LinkedBlockingQueue<byte[]> events, Map conf, TopologyContext context, String zkStr, String zkRoot) {
        _zkStr = zkStr;
        _zkRoot = zkRoot;
        _events = events;
        
        Number putTimeout = (Number) conf.get("scribe.spout.put.timeout");
        if(putTimeout==null) putTimeout = 5;
        
        Number portDelta = (Number) conf.get("scribe.spout.port.delta");
        if(portDelta==null) portDelta = 2000;
        
        int port = context.getThisWorkerPort() + portDelta.intValue();

        String host;
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
        
        try {
            _zk =  CuratorFrameworkFactory.newClient(
                    _zkStr,
                    30000,
                    15000,
                    new RetryNTimes(4, 1000));
            _zk.start();
            _zk.create()
               .creatingParentsIfNeeded()
               .withMode(CreateMode.EPHEMERAL)
               .forPath(_zkRoot + "/" + host + ":" + port);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
        ScribeServiceHandler handler = new ScribeServiceHandler(putTimeout.intValue());
        try {
            //TODO: more efficient as just a nonblockingserver?
            THsHaServer.Args args = new THsHaServer.Args(new TNonblockingServerSocket(port))
                    .workerThreads(1)
                    .protocolFactory(new TBinaryProtocol.Factory())
                    .processor(new scribe.Processor(handler));

            _server = new THsHaServer(args);
            Thread thread = new Thread(new Runnable() {
                    public void run() {
                      _server.serve();
                    }
                  });
            thread.setDaemon(true);
            thread.start();
        } catch (TTransportException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public void shutdown() {
        _server.stop();
        _zk.close();        
    }
    
    public void activate() {
        _active = true;
    }

    public void deactivate() {
        _active = false;
        while(true) {
            synchronized(_scribeLock) {
                if(_scribeActive) {
                    Utils.sleep(10);
                } else {
                    break;
                }
            }
        }
    }
    
    private class ScribeServiceHandler implements scribe.Iface {
        long _startTime;
        int _putTimeoutSecs;
        
        public ScribeServiceHandler(int putTimeoutSecs) {
            _startTime = System.currentTimeMillis();
            _putTimeoutSecs = putTimeoutSecs;
        }
        
        @Override
        public ResultCode Log(List<LogEntry> messages) throws TException {
            synchronized(_scribeLock) {
                if(!_active) return ResultCode.TRY_LATER;
                _scribeActive = true;
            }
            for(LogEntry le: messages) {
                try {
                    byte[] o = Base64.decodeBase64(le.get_message());
                    boolean taken = _events.offer(o, _putTimeoutSecs, TimeUnit.SECONDS);
                    if(!taken) return ResultCode.TRY_LATER;
                } catch(InterruptedException ex) {
                    throw new TException(ex);
                }
            }
            //TODO: for a reliable spout, need to *block* (with timeout) here until every single tuple gets acked
            _scribeActive = false;
            return ResultCode.OK;
        }

        @Override
        public void shutdown() throws TException {
            
        }

        @Override
        public String getName() throws TException {
            return "rainbird-scribe-spout";
        }

        @Override
        public String getVersion() throws TException {
            return "0.0.1";
        }

        @Override
        public fb_status getStatus() throws TException {
            return fb_status.ALIVE;
        }

        @Override
        public String getStatusDetails() throws TException {
            return "n/a";
        }

        @Override
        public Map<String, Long> getCounters() throws TException {
            return new HashMap<String, Long>();
        }

        @Override
        public long getCounter(String key) throws TException {
            return 0L;
        }

        @Override
        public void setOption(String key, String value) throws TException {
        }

        @Override
        public String getOption(String key) throws TException {
            return "";
        }

        @Override
        public Map<String, String> getOptions() throws TException {
            return new HashMap<String, String>();
        }

        @Override
        public String getCpuProfile(int profileDurationInSec) throws TException {
            return "n/a";
        }

        @Override
        public long aliveSince() throws TException {
            return _startTime;
        }

        @Override
        public void reinitialize() throws TException {
        }
        
    }
}
