package org.pentaho.di.trans.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.pentaho.di.core.exception.KettleException;

import java.util.concurrent.Callable;

/**
 * Kafka reader callable
 *
 * @author Michael Spector
 */
public abstract class KafkaConsumerCallable implements Callable<Object> {

    private KafkaConsumerData data;
    private KafkaConsumerMeta meta;
    private KafkaConsumer step;

    public KafkaConsumerCallable(KafkaConsumerMeta meta, KafkaConsumerData data, KafkaConsumer step) {
        this.meta = meta;
        this.data = data;
        this.step = step;
    }

    /**
     * Called when new message arrives from Kafka stream
     *
     * @param message Kafka message
     * @param key     Kafka key
     */
    protected abstract void messageReceived(byte[] key, byte[] message) throws KettleException;

    public Object call() throws KettleException {
        long limit;
        String strData = meta.getLimit();

        limit = getLimit(strData);
        if (limit > 0) {
            step.logDebug("Collecting up to " + limit + " messages");
        } else {
            step.logDebug("Collecting unlimited messages");
        }
        while (!data.canceled && (limit <= 0 || data.processed < limit)) {
            ConsumerRecords<Object, Object> records = data.consumer.poll(1000);
            for (ConsumerRecord<Object, Object> record : records) {
                messageReceived((byte[]) record.key(), (byte[]) record.value());
                ++data.processed;
            }
            data.consumer.commitSync();
        }
        // Notify that all messages were read successfully
        step.setOutputDone();
        return null;
    }

    private long getLimit(String strData) throws KettleException {
        long limit;
        try {
            limit = KafkaConsumerMeta.isEmpty(strData) ? 0 : Long.parseLong(step.environmentSubstitute(strData));
        } catch (NumberFormatException e) {
            throw new KettleException("Unable to parse messages limit parameter", e);
        }
        return limit;
    }
}
