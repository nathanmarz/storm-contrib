package storm.growl;

import java.util.ArrayList;
import java.util.List;

import net.sf.libgrowl.internal.IProtocol;

public class GrowlConfig
{
	/*
	 * member variables
	 */
	private String _name 		= "StormGrowl";
	private String _typeId 		= "stormgrowl";
	
	
	public List<String> hosts 	= null;
	public int port 			= IProtocol.DEFAULT_GROWL_PORT;
	public boolean sticky		= false;
	public int priority 		= 0;
	public String iconUrl		= "null";
	
	
	public GrowlConfig(){
		hosts = new ArrayList<String>();
	}
	
	public GrowlConfig(String host){
		hosts = new ArrayList<String>();
		hosts.add(host);
	}
	
	public GrowlConfig(List<String> hosts){
		this.hosts = hosts; 
	}
	
	public void addHost(String host){
		this.hosts.add(host);
	}
	
	public String getName(){
		return _name;
	}
	public String getTypeId(){
		return _typeId;
	}
}	