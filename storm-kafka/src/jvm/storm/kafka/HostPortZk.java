package storm.kafka;

public class HostPortZk extends HostPort {

    public HostPortZk(String host, int port) {
        super(host, port);
    }

    public HostPortZk(String host) {
        super(host, 2181);
    }

    public String toString() {
        return host + ":" + port;
    }

}
