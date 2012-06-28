package storm.kafka;

import java.util.HashMap;
import java.util.Map;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaPartitionConnections {
    Map<Integer, SimpleConsumer> _kafka = new HashMap<Integer, SimpleConsumer>();
    KafkaConfig _config;

    public KafkaPartitionConnections(KafkaConfig conf) {
        _config = conf;
    }

    public SimpleConsumer getConsumer(int partition) {
        int hostIndex = partition / _config.partitionsPerHost;
        if(!_kafka.containsKey(hostIndex)) {
            HostPort hp = _config.hosts.get(hostIndex);
            _kafka.put(hostIndex, new SimpleConsumer(hp.host, hp.port, _config.socketTimeoutMs, _config.bufferSizeBytes));

        }
        return _kafka.get(hostIndex);
    }

    public int getHostPartition(int globalPartition) {
        return globalPartition % _config.partitionsPerHost;
    }

    public int getNumberOfHosts() {
        return _config.hosts.size();
    }

    public void close() {
        for(SimpleConsumer consumer: _kafka.values()) {
            consumer.close();
        }
    }
}
