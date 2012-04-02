package backtype.storm.contrib.cassandra.bolt.determinable;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;

public interface RowKeyDeterminable extends Serializable {
	/**
	 * Given a <code>backtype.storm.tuple.Tuple</code> generate a Cassandra
	 * row key.
	 * 
	 * @param tuple
	 * @return
	 */
	Object determineRowKey(Tuple tuple);
}
