package backtype.storm.contrib.cassandra.bolt.determinable;

import backtype.storm.tuple.Tuple;

public class DefaultRowKeyDeterminable implements RowKeyDeterminable {
	private String keyField;
	
	public DefaultRowKeyDeterminable(String keyField){
		this.keyField = keyField;
	}

	@Override
	public Object determineRowKey(Tuple tuple) {
		return tuple.getValueByField(this.keyField);
	}

}
