package backtype.storm.contrib.cassandra.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * Abstract <code>IRichBolt</code> implementation that caches/batches
 * <code>backtype.storm.tuple.Tuple</code> and processes them on a separate
 * thread.
 * <p/>
 * <p/>
 * Subclasses are obligated to implement the
 * <code>executeBatch(List<Tuple> inputs)</code> method, called when a batch of
 * tuples should be processed.
 * <p/>
 * Subclasses that overide the <code>prepare()</code> and <code>cleanup()</code>
 * methods <b><i>must</i></b> call the corresponding methods on the superclass
 * (i.e. <code>super.prepare()</code> and <code>super.cleanup()</code>) to
 * ensure proper initialization and termination.
 * 
 * 
 * 
 * 
 * @author ptgoetz
 * 
 */
@SuppressWarnings("serial")
public abstract class AbstractBatchingBolt implements IRichBolt,
		CassandraConstants {

	private static final Logger LOG = LoggerFactory
			.getLogger(AbstractBatchingBolt.class);
	
	private boolean ackOnReceive = false;
	
	private OutputCollector collector;

	private LinkedBlockingQueue<Tuple> queue;

	private BatchThread batchThread;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.queue = new LinkedBlockingQueue<Tuple>();
		this.batchThread = new BatchThread();
		this.batchThread.start();
	}
	
	public void setAckOnReceive(boolean ackOnReceive){
		this.ackOnReceive = ackOnReceive;
	}

	@Override
	public final void execute(Tuple input) {
		if(this.ackOnReceive){
			this.collector.ack(input);
		}
		this.queue.offer(input);
	}

	@Override
	public void cleanup() {
		this.batchThread.stopRunning();
	}

	/**
	 * Process a <code>java.util.List</code> of
	 * <code>backtype.storm.tuple.Tuple</code> objects that have been
	 * cached/batched.
	 * <p/>
	 * This method is analagous to the <code>execute(Tuple input)</code> method
	 * defined in the bolt interface. Subclasses are responsible for processing
	 * and/or ack'ing tuples as necessary. The only difference is that tuples
	 * are passed in as a list, as opposed to one at a time.
	 * <p/>
	 * 
	 * 
	 * @param inputs
	 */
	public abstract void executeBatch(List<Tuple> inputs);

	private class BatchThread extends Thread {

		boolean stopRequested = false;

		BatchThread() {
			super("batch-bolt-thread");
			super.setDaemon(true);
		}

		@Override
		public void run() {
			while (!stopRequested) {
			    try {
			        ArrayList<Tuple> batch = new ArrayList<Tuple>();
			        // drainTo() does not block, take() does.
			        Tuple t = queue.take();
			        batch.add(t);
	                queue.drainTo(batch);
	                executeBatch(batch);
                    
                }
                catch (InterruptedException e) {}				
			}
		}

		synchronized void stopRunning() {
			this.stopRequested = true;
		}
	}
}
