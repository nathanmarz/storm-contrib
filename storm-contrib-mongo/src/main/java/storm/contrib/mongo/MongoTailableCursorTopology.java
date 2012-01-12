package storm.contrib.mongo;

import static backtype.storm.utils.Utils.tuple;

import java.util.Date;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;

/**
 * 
 * Quick and dirty test. Parts of this should probably be turned into a proper
 * integration test.
 * 
 * @author Dan Beaulieu
 *
 */
public class MongoTailableCursorTopology {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		
		// mongo stuff
        Mongo mongo = new Mongo(new MongoURI("mongodb://127.0.0.1:27017"));
        mongo.dropDatabase("mongo_storm_tailable_cursor");
        final BasicDBObject options = new BasicDBObject("capped", true);
        options.put("size", 10000);
        mongo.getDB("mongo_storm_tailable_cursor").createCollection("test", options);
        final DBCollection coll = mongo.getDB("mongo_storm_tailable_cursor").getCollection("test");
        
		TopologyBuilder builder = new TopologyBuilder();
        MongoSpout spout = new MongoSpout("localhost", 27017, "mongo_storm_tailable_cursor", "test", new BasicDBObject()) {

        	/**
			 * Ugh.
			 */
			private static final long serialVersionUID = 1L;

			@Override
        	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        		
        		declarer.declare(new Fields("document"));	
        	}

			@Override
			public List<Object> dbObjectToStormTuple(DBObject document) {
				
				return tuple(document);
			}
        	
        };
        builder.setSpout("1", spout);

        
        Config conf = new Config();
        conf.setDebug(true);
        
        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Runnable writer = new Runnable() {

			@Override
			public void run() {
				for (int i=0; i < 1000; i++) {
	                final BasicDBObject doc = new BasicDBObject("_id", i);
	                doc.put("ts", new Date());
	                coll.insert(doc);
	            }
			}
		};
		
		new Thread(writer).start();
		
        Utils.sleep(10000);
        cluster.shutdown();
        
        mongo.dropDatabase("mongo_storm_tailable_cursor");

	}

}
