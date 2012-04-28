package backtype.storm.contrib.cassandra.bolt.determinable;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;

public interface ColumnFamilyDeterminable extends Serializable{

	/**
	 * Given a <code>backtype.storm.tuple.Tuple</code> object, 
	 * determine the column family to write to.
	 * 
	 * @param tuple
	 * @return
	 */
	public String determineColumnFamily(Tuple tuple);
}
