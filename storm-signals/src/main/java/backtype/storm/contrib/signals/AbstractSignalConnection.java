package backtype.storm.contrib.signals;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;

public abstract class AbstractSignalConnection implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSignalConnection.class);
    static final String namespace = "storm-signals";
    protected String name;
    protected CuratorFramework client;
    protected SignalListener listener;
    
    
    protected void initWatcher() throws Exception {
        // create base path if necessary
        Stat stat = this.client.checkExists().usingWatcher(this).forPath(this.name);
        if (stat == null) {
            String path = this.client.create().creatingParentsIfNeeded().forPath(this.name);
            LOG.info("Created: " + path);
        }
    }

    @Override
    public void process(WatchedEvent we) {
        try {
            this.client.checkExists().usingWatcher(this).forPath(this.name);
            LOG.debug("Renewed watch for path {}", this.name);
        } catch (Exception ex) {
            LOG.error("Error renewing watch.", ex);
        }

        switch (we.getType()) {
        case NodeCreated:
            LOG.debug("Node created.");
            break;
        case NodeDataChanged:
            LOG.debug("Received signal.");
            try {
                this.listener.onSignal(this.client.getData().forPath(we.getPath()));
            } catch (Exception e) {
                LOG.warn("Unable to process signal.", e);
            }
            break;
        case NodeDeleted:
            LOG.debug("NodeDeleted");
            break;
        }
    }

    public void close() {
        this.client.close();
    }
    
    
    public void send(String toPath, byte[] signal) throws Exception {
        Stat stat = this.client.checkExists().forPath(toPath);
        if (stat == null) {
            String path = this.client.create().creatingParentsIfNeeded().forPath(toPath);
            LOG.info("Created: " + path);
        }
        this.client.setData().forPath(toPath, signal);
    }
}
