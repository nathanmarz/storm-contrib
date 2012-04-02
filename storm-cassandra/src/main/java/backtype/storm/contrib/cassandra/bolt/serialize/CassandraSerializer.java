package backtype.storm.contrib.cassandra.bolt.serialize;

import backtype.storm.tuple.Tuple;

public interface CassandraSerializer {
	public void writeToCassandra(Tuple tuple);
}
