package storm.growl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.libgrowl.Application;
import net.sf.libgrowl.GrowlConnector;
import net.sf.libgrowl.Notification;
import net.sf.libgrowl.NotificationType;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GrowlBolt extends BaseBasicBolt
{
	public static Logger LOG = Logger.getLogger(GrowlBolt.class);
	/*
	 * Growl Conf
	 */
	String name 				="";
	String notificationTypeId 	= "";
	List<String> hosts 			= null;
	int port 					= 0;
	
	/* use sticky notification or not. */
	boolean sticky 				= false;
	/* notification priority */
	int priority 				= 0;
	String iconUrl				= null;
	
	/*
	 * Growl Objects
	 */
	Application _application;
	NotificationType _notificationType;
	NotificationType[] _notificationTypes;
	List<GrowlConnector> _growls;
	

	/* 
	 * Constructor
	 */
    public GrowlBolt(GrowlConfig growlConf) {
    	name = growlConf.getName();
    	notificationTypeId = growlConf.getTypeId();
    	
    	if(growlConf.hosts.isEmpty()){
    		LOG.warn("No host registered in GrowlConfig. Use \"localhost\".");
    		hosts = new ArrayList<String>();
    		hosts.add("localhost");
    	}else{
    		hosts = growlConf.hosts;
    	}
    	port = growlConf.port;
    	priority = growlConf.priority;
    	sticky = growlConf.sticky;
    	iconUrl = growlConf.iconUrl;
    	
    }    
    
    @SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
    	_application = new Application(name);
    	_notificationType = new NotificationType(notificationTypeId, name, iconUrl);
    	_notificationTypes = new NotificationType[] { _notificationType};
    	
    	_growls = new ArrayList<GrowlConnector>();
    	for(String host : hosts){
    		GrowlConnector growl = new GrowlConnector(host, port);
    		growl.register(_application, _notificationTypes);
    		_growls.add(growl);
    	}
	}
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        /*
         * tuple must contains Fields named "title" and "message".
         * "title" is used for Growl Title
         * "message" is used for Growl Message
         */
    	
    	String title = tuple.getStringByField("title");
    	String message = tuple.getStringByField("message");    	
    	
    	Notification notification = new Notification(_application, _notificationType, title, message);
		notification.setPriority(priority);
		notification.setSticky(sticky);
		
		for(GrowlConnector growl : _growls){
			growl.notify(notification);
		}
		collector.emit(new Values(title, message));

    }	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("title", "message"));
    }
    
}