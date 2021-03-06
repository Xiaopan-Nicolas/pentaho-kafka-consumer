package com.ruckuswireless.pentaho.kafka.consumer;

import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.pentaho.di.core.exception.KettleException;

import kafka.consumer.ConsumerTimeoutException;
import kafka.message.MessageAndMetadata;

/**
 * Kafka reader callable
 *
 * @author Michael Spector
 */
public abstract class KafkaConsumerCallable implements Callable<Object> {

	private KafkaConsumerData data;
	private KafkaConsumerMeta meta;
	private KafkaConsumerStep step;

	public KafkaConsumerCallable(KafkaConsumerMeta meta, KafkaConsumerData data, KafkaConsumerStep step) {
		this.meta = meta;
		this.data = data;
		this.step = step;
	}

	/**
	 * Called when new message arrives from Kafka stream
	 *
	 * @param message
	 *            Kafka message
	 * @param key
	 *            Kafka key
	 */
	protected abstract void messageReceived(String key, String message) throws KettleException;

	public Object call() throws KettleException {
		try {
			long limit;
			String strData = meta.getLimit();

			try {
				limit = KafkaConsumerMeta.isEmpty(strData) ? 0 : Long.parseLong(step.environmentSubstitute(strData));
			} catch (NumberFormatException e) {
				throw new KettleException("Unable to parse messages limit parameter", e);
			}
			if (limit > 0) {
				step.logDebug("Collecting up to " + limit + " messages");
			} else {
				step.logDebug("Collecting unlimited messages");
			}
			while (!data.canceled && (limit <= 0 || data.processed < limit)) {
				// 消息消费请求
				ConsumerRecords<String, String> records = data.consumer.poll(1000);
				for (ConsumerRecord<String, String> record : records){
					ConsumerRecord<String, String> messageAndMetadata = record;
					messageReceived(messageAndMetadata.key(), messageAndMetadata.value());
					++data.processed;
				}
			}
		} catch (ConsumerTimeoutException cte) {
			step.logDebug("Received a consumer timeout after " + data.processed + " messages");
			if (!meta.isStopOnEmptyTopic()) {
				// Because we're not set to stop on empty, this is an abnormal
				// timeout
				throw new KettleException("Unexpected consumer timeout!", cte);
			}
		}
		// Notify that all messages were read successfully
		data.consumer.commitSync();
		step.setOutputDone();
		return null;
	}
}
