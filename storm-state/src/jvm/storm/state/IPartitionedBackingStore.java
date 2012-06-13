package storm.state;

import java.io.Serializable;


public interface IPartitionedBackingStore extends Serializable {
    public void init();
    public void storeMeta(String meta);
    public String getMeta();
    public IBackingStore getBackingStore(int partition);
}
