package storm.contrib.sqs;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.utils.Utils;

/**
 * A Spout which consumes an Amazon SQS queue.
 * 
 * Subclasses should simply override two methods:
 * <ul>
 *   <li>{@link #declareOutputFields(OutputFieldsDeclarer) declareOutputFields}
 *   <li>{@link #messageToStormTuple(Message) messageToStormTuple}, which turns
 *   an SQS message into a Storm tuple matching the declared output fields.
 * </ul>
 * 
 * This Spout also comes in two "flavors" -- reliable and unreliable.
 * 
 * The unreliable version deletes messages from SQS as soon as they are consumed,
 * and therefore tuples cannot be replayed. This version does not anchor tuples.
 * 
 * The reliable version waits until a tuple has been completely {@code ack}ed
 * before deleting it from SQS. If the tuple {@code fail}s, then the visibility
 * timeout on that message is reset to 0, so it will be replayed immediately.
 * This matches Storm's own reliability model remarkably well.
 * 
 * <p>
 * <b>WARNING:</b> You should ensure that the VisibilityTimeout parameter
 * of your Amazon SQS queue is (significantly, in order to account for 
 * network delays) longer than Storm's {@code TOPOLOGY_MESSAGE_TIMEOUT_SECS}
 * parameter. Otherwise, it is possible that a message which Storm is taking a
 * long time to process could be made visible again by SQS, and consumed again
 * by Storm, before {@code ack} or {@code fail} have a chance to be called.
 * 
 * @author Adrian Petrescu <apetresc@gmail.com>
 *
 */
public abstract class SqsQueueSpout implements IRichSpout {
	private SpoutOutputCollector collector;
	private AmazonSQSAsync sqs;
	
	private final String queueUrl;
	private final boolean reliable;
	private LinkedBlockingQueue<Message> queue;

	/**
	 * @param queueUrl the URL for the Amazon SQS queue to consume from
	 * @param reliable whether this spout uses Storm's reliability facilities
	 */
	public SqsQueueSpout(String queueUrl, boolean reliable) {
		this.queueUrl = queueUrl;
		this.reliable = reliable;
	}

	@Override
	public void open(
			@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
	
		this.collector = collector;
		queue = new LinkedBlockingQueue<Message>();
		try {
			sqs = new AmazonSQSAsyncClient(new PropertiesCredentials(SqsQueueSpout.class.getResourceAsStream("/AwsCredentials.properties")));
		} catch (IOException ioe) {
			throw new RuntimeException("Couldn't load AWS credentials", ioe);
		}
	}

	@Override
	public void nextTuple() {
		if (queue.isEmpty()) {
			ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(
					new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(10));
			queue.addAll(receiveMessageResult.getMessages());
		}
		
		
		Message message = queue.poll();
		if (message != null) {
			if (reliable) {
				collector.emit(getStreamId(message), messageToStormTuple(message), message.getReceiptHandle());
			} else {
				// Delete it right away
				sqs.deleteMessageAsync(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));

				collector.emit(getStreamId(message), messageToStormTuple(message));
			}
		} else {
			// Still empty, go to sleep.
			Utils.sleep(100);
		}
	}

	/**
	 * Returns the stream on which this spout will emit. By default, it is just
	 * {@code Utils.DEFAULT_STREAM_ID}. Simply override this method to send to
	 * a different stream.
	 * 
	 * By using the {@code message} parameter, you can send different messages
	 * to different streams based on context.
	 *
	 * @return the stream on which this spout will emit.
	 */
	public String getStreamId(Message message) {
		return Utils.DEFAULT_STREAM_ID;
	}
	
	@Override
	public void ack(Object msgId) {
		// Only called in reliable mode.
		try {
			sqs.deleteMessageAsync(new DeleteMessageRequest(queueUrl, (String) msgId));
		} catch (AmazonClientException ace) { }
	}

	@Override
	public void fail(Object msgId) {
		// Only called in reliable mode.
		try {
			sqs.changeMessageVisibilityAsync(
					new ChangeMessageVisibilityRequest(queueUrl, (String) msgId, 0));
		} catch (AmazonClientException ace) { }
	}
	
	public abstract List<Object> messageToStormTuple(Message message);

	@Override
	public void close() {
		sqs.shutdown();
		// Works around a known bug in the Async clients
		// @see https://forums.aws.amazon.com/thread.jspa?messageID=305371
		((AmazonSQSAsyncClient) sqs).getExecutorService().shutdownNow();
	}

	@Override
	public boolean isDistributed() {
		return false;
	}

}
