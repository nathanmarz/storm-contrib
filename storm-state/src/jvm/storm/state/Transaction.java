package storm.state;

public interface Transaction<T> {
    public Object apply(T state);
}
