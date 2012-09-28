package storm.kafka;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZooKeeperDiscovery implements Watcher {

    public void process(WatchedEvent event) {
    }

    public static List<HostPort> loadZkBrokers(List<String> zkHosts, int zkTimeout, String zkBrokersRoot) {
        List<HostPortZk> _hosts = KafkaConfig.convertHostsZk(zkHosts);
        List<String> discoveredHosts = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (HostPortZk hpzk: _hosts) {
            if (!first) sb.append(",");
            sb.append( hpzk.toString() );
            first = false;
        }
        try {
            ZooKeeper zk = new ZooKeeper(sb.toString(), zkTimeout, new ZooKeeperDiscovery());
            try {
                if ( zk.exists(zkBrokersRoot, false) != null ) {

                    try {
                        List<String> brokers = zk.getChildren(zkBrokersRoot, false);
                        for (String broker: brokers) {

                            try {
                                String data = new String( zk.getData(zkBrokersRoot+"/"+broker, false, null) );
                                discoveredHosts.add(data.substring(data.indexOf(":")+1));
                            } catch (InterruptedException ex) {
                                try {
                                    zk.close();
                                } catch (InterruptedException ex3) { /* ignore */ }
                                throw new IllegalStateException("ZooKeeper connection exception while loading data for " + zkBrokersRoot + "/" + broker + ".");
                            } catch (KeeperException ex2) {
                                try {
                                    zk.close();
                                } catch (InterruptedException ex3) { /* ignore */ }
                                throw new IllegalStateException("ZooKeeper exception while loading data for " + zkBrokersRoot + "/" + broker + ".");
                            }

                        }
                        try {
                            zk.close();
                        } catch (InterruptedException ex3) { /* ignore */ }
                        return KafkaConfig.convertHosts(discoveredHosts);
                    } catch (InterruptedException ex) {
                        try {
                            zk.close();
                        } catch (InterruptedException ex3) { /* ignore */ }
                        throw new IllegalStateException("ZooKeeper connection exception while loading children of " + zkBrokersRoot + ".");
                    } catch (KeeperException ex2) {
                        try {
                            zk.close();
                        } catch (InterruptedException ex3) { /* ignore */ }
                        throw new IllegalStateException("ZooKeeper exception while loading children of " + zkBrokersRoot + ".");
                    }

                } else {
                    try {
                        zk.close();
                    } catch (InterruptedException ex3) { /* ignore */ }
                    throw new IllegalStateException(zkBrokersRoot + " not found on the discovery ZooKeeper.");
                }
            } catch (InterruptedException ex) {
                try {
                    zk.close();
                } catch (InterruptedException ex3) { /* ignore */ }
                throw new IllegalStateException("ZooKeeper connection exception while checking if the brokers path exists.");
            } catch (KeeperException ex2) {
                try {
                    zk.close();
                } catch (InterruptedException ex3) { /* ignore */ }
                throw new IllegalStateException("ZooKeeper exception while checking if the brokers path exists.");
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Can't connect to the discovery ZooKeeper.");
        }
    }

}
