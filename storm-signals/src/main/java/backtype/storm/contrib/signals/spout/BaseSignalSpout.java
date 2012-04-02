// Copyright (c) P. Taylor Goetz (ptgoetz@gmail.com)

package backtype.storm.contrib.signals.spout;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

@SuppressWarnings("serial")
public abstract class BaseSignalSpout extends BaseRichSpout implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(BaseSignalSpout.class);
    private static final String namespace = "storm-signals";
    private String name;
    private CuratorFramework client;

    public BaseSignalSpout(String name) {
        this.name = name;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            initZookeeper(conf);
        } catch (Exception e) {
            LOG.error("Error creating zookeeper client.", e);
        }
    }

    @SuppressWarnings("rawtypes")
    private void initZookeeper(Map conf) throws Exception {
        String connectString = zkHosts(conf);
        int retryCount = (Integer) conf.get("storm.zookeeper.retry.times");
        int retryInterval = (Integer) conf.get("storm.zookeeper.retry.interval");

        this.client = CuratorFrameworkFactory.builder().namespace(namespace).connectString(connectString)
                .retryPolicy(new RetryNTimes(retryCount, retryInterval)).build();
        this.client.start();

        // create base path if necessary
        Stat stat = this.client.checkExists().usingWatcher(this).forPath(this.name);
        if (stat == null) {
            String path = this.client.create().creatingParentsIfNeeded().forPath(this.name);
            LOG.info("Created: " + path);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private String zkHosts(Map conf) {
        int zkPort = (Integer) conf.get("storm.zookeeper.port");
        List<String> zkServers = (List<String>) conf.get("storm.zookeeper.servers");

        Iterator<String> it = zkServers.iterator();
        StringBuffer sb = new StringBuffer();
        while (it.hasNext()) {
            sb.append(it.next());
            sb.append(":");
            sb.append(zkPort);
            if (it.hasNext()) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    /**
     * Releases the zookeeper connection.
     */
    @Override
    public void close() {
        super.close();
        this.client.close();
    }

    @Override
    public void process(WatchedEvent we) {
        try {
            this.client.checkExists().usingWatcher(this).forPath(this.name);
            LOG.debug("Renewed watch for path %s", this.name);
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
                this.onSignal(this.client.getData().forPath(we.getPath()));
            } catch (Exception e) {
                LOG.warn("Unable to process signal.", e);
            }
            break;
        case NodeDeleted:
            LOG.debug("NodeDeleted");
            break;
        }

    }

    protected abstract void onSignal(byte[] data);

}
