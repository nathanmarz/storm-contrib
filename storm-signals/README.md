## Storm-Signals
Storm-Signals aims to provide a way to send messages ("signals") to components (spouts/bolts) in a storm topology that are otherwise not addressable. 

Storm topologies can be considered static in that modifications to a topology's behavior require redeployment. Storm-Signals provides a simple way to modify a topology's behavior at runtime, without redeployment.



### Use Cases
Typical storm spouts run forever (until undeployed), emitting tuples based on an underlying, presumably event-driven, data source/stream.

Some storm users have expressed an interest in having more control over that pattern, for instance in situations where the data stream is not open-ended, or where the situation requires that data streams be controllable (i.e. the ability to start/stop/pause/resume processing).

Storm-Signals provides a very simple mechanism for communicating with spouts deployed within a storm topology. The comunication mechanism resides outside of storm's basic stream processing paradigm (i.e. calls to `nextTuple()` and the tuple ack/fail mechanism).

Signals (messages)

#### Sample Use Cases

* Ability to start/stop/pause/resume a spout from a process _external to_ the storm topology.
* Ability to change the source of a spout's stream without redeploying the topology.
* Initiating processing of a set/batch of data based on a schedule (such as a Quartz or cron job)
* Periodically sending a dynamic SQL query to a spout that emits tuples for processing.
* Any other use case you can think of. :)

## Usage

### Spout Implementation
Currently (Version 0.1.0) provides a basic abstract `BaseRichSpout` implementation that must be subclassed:

`backtype.storm.contrib.signals.spout.BaseSignalSpout`

Subclasses _must_ override the `onSignal()` method:

	protected abstract void onSignal(byte[] data);

This method is called when a signal is sent to a spout. The signal payload is a `byte[]` that can contain anything (string, data, seriliazed object(s), etc.).

Subclasses _must_ override the superclass constructor:

	public TestSignalSpout(String name) {
	    super(name);
	}
	
The `name` parameter provides a unique ID for the spout that allows `SingalClient`s to address the bolt and send it messages.

Subclasses _must_ call `super.open()` if they override the `open()` method: 

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    super.open(conf, context, collector);
	}

Failure to do so will prevent the spout from receiving signals (i.e. `onSignal()` will never be called).

### Signal Client

The `SignalClient` constructor requires two arguments:

1. a zookeeper connect string ("host1:port1,host2:port2,hostN:portN") that should match the storm zookeeper configuration
2. a name string (this should match the name used to construct the `BaseSignalSpout` subclass)
	
Example:

	public static void main(String[] args) throws Exception {
	    SignalClient sc = new SignalClient("localhost:2181", "test-signal-spout");
	    sc.start();
	    try {
	        sc.send("Hello Signal Spout!".getBytes());
	    } finally {
	        sc.close();
	    }
	}


## Maven Usage

### Maven Dependency

Point (non-SNAPSHOT) releases will be available on maven central.

	<dependency>
		<groupId>com.github.ptgoetz</groupId>
		<artifactId>storm-signals</artifactId>
		<version>0.1.0</version>
	</dependency>





