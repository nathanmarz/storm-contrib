package storm.contrib.mongo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.utils.Utils;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
* A Spout which consumes documents from a Mongodb tailable cursor.
*
* Subclasses should simply override two methods:
* <ul>
* <li>{@link #declareOutputFields(OutputFieldsDeclarer) declareOutputFields}
* <li>{@link #dbObjectToStormTuple(DBObject) dbObjectToStormTuple}, which turns
* a Mongo document into a Storm tuple matching the declared output fields.
* </ul>
*
** <p>
* <b>WARNING:</b> You can only use tailable cursors on capped collections.
* 
* @author Dan Beaulieu <danjacob.beaulieu@gmail.com>
*
*/
public abstract class MongoSpout implements IRichSpout {

	private SpoutOutputCollector collector;
	
	private LinkedBlockingQueue<DBObject> queue;
	private final AtomicBoolean opened = new AtomicBoolean(false);
	
	private DB mongoDB;
	private final DBObject query;
	
	private final String mongoHost;
	private final int mongoPort;
	private final String mongoDbName;
	private final String mongoCollectionName;
	
	
	public MongoSpout(String mongoHost, int mongoPort, String mongoDbName, String mongoCollectionName, DBObject query) {
		
		this.mongoHost = mongoHost;
		this.mongoPort = mongoPort;
		this.mongoDbName = mongoDbName;
		this.mongoCollectionName = mongoCollectionName;
		this.query = query;
	}
	
	class TailableCursorThread extends Thread {
		
		LinkedBlockingQueue<DBObject> queue;
		String mongoCollectionName;
		DB mongoDB;
		DBObject query;

		public TailableCursorThread(LinkedBlockingQueue<DBObject> queue, DB mongoDB, String mongoCollectionName, DBObject query) {
			
			this.queue = queue;
			this.mongoDB = mongoDB;
			this.mongoCollectionName = mongoCollectionName;
			this.query = query;
		}

		public void run() {
			
			while(opened.get()) {
				try {
					// create the cursor
					mongoDB.requestStart();
					final DBCursor cursor = mongoDB.getCollection(mongoCollectionName)
												.find(query)
												.sort(new BasicDBObject("$natural", 1))
												.addOption(Bytes.QUERYOPTION_TAILABLE)
												.addOption(Bytes.QUERYOPTION_AWAITDATA);
					try {
						while (opened.get() && cursor.hasNext()) {
		                    final DBObject doc = cursor.next();
		
		                    if (doc == null) break;
		
		                    queue.put(doc);
		                }
					} finally {
						try { 
							if (cursor != null) cursor.close(); 
						} catch (final Throwable t) { }
	                    try { 
	                    	mongoDB.requestDone(); 
	                    	} catch (final Throwable t) { }
	                }
					
					Utils.sleep(500);
				} catch (final MongoException.CursorNotFound cnf) {
					// rethrow only if something went wrong while we expect the cursor to be open.
                    if (opened.get()) {
                    	throw cnf;
                    }
                } catch (InterruptedException e) { break; }
			}
		};
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		this.collector = collector;
		this.queue = new LinkedBlockingQueue<DBObject>(1000);
		try {
			this.mongoDB = new Mongo(this.mongoHost, this.mongoPort).getDB(this.mongoDbName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		TailableCursorThread listener = new TailableCursorThread(this.queue, this.mongoDB, this.mongoCollectionName, this.query);
		this.opened.set(true);
		listener.start();
	}

	@Override
	public void close() {
		this.opened.set(false);
	}

	@Override
	public void nextTuple() {
		
		DBObject dbo = this.queue.poll();
		if(dbo == null) {
            Utils.sleep(50);
        } else {
            this.collector.emit(dbObjectToStormTuple(dbo));
        }
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub	
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub	
	}

	@Override
	public boolean isDistributed() {
		// TODO Auto-generated method stub
		return false;
	}
	
	public abstract List<Object> dbObjectToStormTuple(DBObject message);

}
