This library implements DFS backed data structures for use within Storm topologies. State is stored in memory but is persisted to the DFS for durability. This library is very efficient, able to sustain very high throughputs of reads and writes.

This library works similarly to HBase: writes are written to a transaction log file open in HDFS. Every so often, the transaction log can be committed which does an HDFS "sync" call which ensures the data written so far to the transaction log is replicated to all nodes. In the background, the transaction logs are compacted by writing snapshots of the current state to HDFS and rotating the log.

`storm.state.example.MapExample` contains an example of using this library in a topology.
