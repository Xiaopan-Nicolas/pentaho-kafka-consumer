package com.ruckuswireless.pentaho.kafka.consumer;

import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Holds data processed by this step
 * 
 * @author Michael
 */
public class KafkaConsumerData extends BaseStepData implements StepDataInterface {

	KafkaConsumer<String,String> consumer;
	Iterator<ConsumerRecord<String, String>> streamIterator;
	RowMetaInterface outputRowMeta;
	RowMetaInterface inputRowMeta;
	boolean canceled;
	int processed;
}
