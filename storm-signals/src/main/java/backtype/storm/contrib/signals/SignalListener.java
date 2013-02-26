package backtype.storm.contrib.signals;

public interface SignalListener {
    void onSignal(byte[] data);
}
