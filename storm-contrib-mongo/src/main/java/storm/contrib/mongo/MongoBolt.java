package storm.contrib.mongo;

import java.util.Map;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * A Spout for recording input tuples to Mongo. Subclasses are expected to
 * provide the logic behind mapping input tuples to Mongo objects.
 * 
 * @todo Provide optional batching of calls.
 *
 * @author Adrian Petrescu <apetresc@gmail.com>
 *
 */
public abstract class MongoBolt implements IRichBolt {
	private OutputCollector collector;
	private DB mongoDB;
	
	private final String mongoHost;
	private final int mongoPort;
	private final String mongoDbName;

	/**
	 * @param mongoHost The host on which Mongo is running.
	 * @param mongoPort The port on which Mongo is running.
	 * @param mongoDbName The Mongo database containing all collections being
	 * written to.
	 */
	protected MongoBolt(String mongoHost, int mongoPort, String mongoDbName) {
		this.mongoHost = mongoHost;
		this.mongoPort = mongoPort;
		this.mongoDbName = mongoDbName;
	}
	
	@Override
	public void prepare(
			@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		
		this.collector = collector;
		try {
			this.mongoDB = new Mongo(mongoHost, mongoPort).getDB(mongoDbName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void execute(Tuple input) {
		if (shouldActOnInput(input)) {
			String collectionName = getMongoCollectionForInput(input);
			DBObject dbObject = getDBObjectForInput(input);
			if (dbObject != null) {
				try {
					mongoDB.getCollection(collectionName).save(dbObject, new WriteConcern(1));
					collector.ack(input);
				} catch (MongoException me) {
					collector.fail(input);
				}
			}
		} else {
			collector.ack(input);
		}
	}

	/**
	 * Decide whether or not this input tuple should trigger a Mongo write.
	 *
	 * @param input the input tuple under consideration
	 * @return {@code true} iff this input tuple should trigger a Mongo write
	 */
	public abstract boolean shouldActOnInput(Tuple input);
	
	/**
	 * Returns the Mongo collection which the input tuple should be written to.
	 *
	 * @param input the input tuple under consideration
	 * @return the Mongo collection which the input tuple should be written to
	 */
	public abstract String getMongoCollectionForInput(Tuple input);
	
	/**
	 * Returns the DBObject to store in Mongo for the specified input tuple.
	 * 
	 * @param input the input tuple under consideration
	 * @return the DBObject to be written to Mongo
	 */
	public abstract DBObject getDBObjectForInput(Tuple input);
	
	@Override
	public void cleanup() {
		this.mongoDB.getMongo().close();
	}

}
