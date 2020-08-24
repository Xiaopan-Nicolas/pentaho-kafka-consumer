package com.ruckuswireless.pentaho.kafka.consumer;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.login.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;

/**
 * Kafka Consumer step processor
 *
 * @author Michael Spector
 */
public class KafkaConsumerStep extends BaseStep implements StepInterface {
	public static final String CONSUMER_TIMEOUT_KEY = "consumer.timeout.ms";

	public KafkaConsumerStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
			Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		super.init(smi, sdi);

		KafkaConsumerMeta meta = (KafkaConsumerMeta) smi;
		KafkaConsumerData data = (KafkaConsumerData) sdi;

		Properties properties = meta.getKafkaProperties();
		Properties substProperties = new Properties();
		for (Entry<Object, Object> e : properties.entrySet()) {
			substProperties.put(e.getKey(), environmentSubstitute(e.getValue().toString()));
		}
		if (meta.isStopOnEmptyTopic()) {

			// If there isn't already a provided value, set a default of 1s
			if (!substProperties.containsKey(CONSUMER_TIMEOUT_KEY)) {
				substProperties.put(CONSUMER_TIMEOUT_KEY, "1000");
			}
		} else {
			if (substProperties.containsKey(CONSUMER_TIMEOUT_KEY)) {
				logError(Messages.getString("KafkaConsumerStep.WarnConsumerTimeout"));
			}
		}
		ConsumerConfig consumerConfig = new ConsumerConfig(substProperties);
		logBasic(Messages.getString("KafkaConsumerStep.CreateKafkaConsumer.Message", consumerConfig.zkConnect()));
		substProperties.put("zookeeper.connect", consumerConfig.zkConnect());
		substProperties.put("group.id", consumerConfig.groupId());
		substProperties.setProperty("client.id",consumerConfig.clientId());
		substProperties.setProperty("zookeeper.session.timeout.ms",String.valueOf(consumerConfig.zkSessionTimeoutMs()));
		substProperties.setProperty("zookeeper.sync.time.ms",String.valueOf(consumerConfig.zkSyncTimeMs()));
		substProperties.setProperty("auto.commit.interval.ms",String.valueOf(consumerConfig.autoCommitIntervalMs()));
		substProperties.setProperty("enable.auto.commit",substProperties.getProperty("auto.commit.enable"));
		// kerberos
		String isSecurity = substProperties.getProperty("isSecureMode");
		if("true".equalsIgnoreCase(isSecurity)){
			if(StringUtils.isBlank(substProperties.getProperty("security.protocol"))){
				substProperties.setProperty("security.protocol","SASL_PLAINTEXT");
			}
			if(StringUtils.isBlank(substProperties.getProperty("sasl.kerberos.service.name"))){
				substProperties.setProperty("sasl.kerberos.service.name","kafka");
			}
			if(StringUtils.isBlank(substProperties.getProperty("kerberos.domain.name"))){
				substProperties.setProperty("kerberos.domain.name","hadoop.hadoop.com");
			}
			if(StringUtils.isBlank(substProperties.getProperty("key.deserializer"))){
				substProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
			}
			if(StringUtils.isBlank(substProperties.getProperty("value.deserializer"))){
				substProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
			}
			System.setProperty("zookeeper.server.principal", substProperties.getProperty("zookeeper.server.principal"));
			String confPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
			String krb5Conf = confPath + "krb5.conf";
			System.setProperty("java.security.krb5.conf", krb5Conf);
			// 华为新 客户端不使用zk
			substProperties.remove("zookeeper.connect");
			substProperties.setProperty("session.timeout.ms","30000");
		}else {
			substProperties.remove("bootstrap.servers");
		}
		Thread.currentThread().setContextClassLoader(null);
		logBasic("此客户端配置的kerberos krb5.conf 配置：" + System.getProperty("java.security.krb5.conf"));
		logBasic("此客户端配置的kerberos jaas.conf 配置：" + System.getProperty("java.security.auth.login.config"));
		Configuration.setConfiguration(null);
		data.consumer = new KafkaConsumer<String, String>(substProperties);
		String topic = environmentSubstitute(meta.getTopic());
		// 订阅
		data.consumer.subscribe(Collections.singletonList(topic));
		return true;
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		KafkaConsumerData data = (KafkaConsumerData) sdi;
		if (data.consumer != null) {
			data.consumer.close();
		}
		super.dispose(smi, sdi);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		Object[] r = getRow();
		if (r == null) {
			/*
			 * If we have no input rows, make sure we at least run once to
			 * produce output rows. This allows us to consume without requiring
			 * an input step.
			 */
			if (!first) {
				setOutputDone();
				return false;
			}
			r = new Object[0];
		} else {
			incrementLinesRead();
		}

		final Object[] inputRow = r;

		KafkaConsumerMeta meta = (KafkaConsumerMeta) smi;
		final KafkaConsumerData data = (KafkaConsumerData) sdi;

		if (first) {
			first = false;
			data.inputRowMeta = getInputRowMeta();
			// No input rows means we just dummy data
			if (data.inputRowMeta == null) {
				data.outputRowMeta = new RowMeta();
				data.inputRowMeta = new RowMeta();
			} else {
				data.outputRowMeta = getInputRowMeta().clone();
			}
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
		}

		try {
			long timeout;
			String strData = meta.getTimeout();

			try {
				timeout = KafkaConsumerMeta.isEmpty(strData) ? 0 : Long.parseLong(environmentSubstitute(strData));
			} catch (NumberFormatException e) {
				throw new KettleException("Unable to parse step timeout value", e);
			}

			logDebug("Starting message consumption with overall timeout of " + timeout + "ms");

			KafkaConsumerCallable kafkaConsumer = new KafkaConsumerCallable(meta, data, this) {
				protected void messageReceived(String key, String message) throws KettleException {
					Object[] newRow = RowDataUtil.addRowData(inputRow.clone(), data.inputRowMeta.size(),
							new Object[] { message, key });
					putRow(data.outputRowMeta, newRow);

					if (isRowLevel()) {
						logRowlevel(Messages.getString("KafkaConsumerStep.Log.OutputRow",
								Long.toString(getLinesWritten()), data.outputRowMeta.getString(newRow)));
					}
				}
			};
			if (timeout > 0) {
				logDebug("Starting timed consumption");
				ExecutorService executor = Executors.newSingleThreadExecutor();
				try {
					Future<?> future = executor.submit(kafkaConsumer);
					try {
						future.get(timeout, TimeUnit.MILLISECONDS);
					} catch (TimeoutException e) {
					} catch (Exception e) {
						throw new KettleException(e);
					}
				} finally {
					executor.shutdown();
				}
			} else {
				logDebug("Starting direct consumption");
				kafkaConsumer.call();
			}
		} catch (KettleException e) {
			if (!getStepMeta().isDoingErrorHandling()) {
				logError(Messages.getString("KafkaConsumerStep.ErrorInStepRunning", e.getMessage()));
				setErrors(1);
				stopAll();
				setOutputDone();
				return false;
			}
			putError(getInputRowMeta(), r, 1, e.toString(), null, getStepname());
		}
		return true;
	}

	public void stopRunning(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		KafkaConsumerData data = (KafkaConsumerData) sdi;
		data.consumer.close();
		data.canceled = true;

		super.stopRunning(smi, sdi);
	}
}
