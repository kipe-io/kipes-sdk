package io.kipe.streams.kafka.examples.zomatorideranalysis.stream;

import io.kipe.streams.kafka.examples.zomatorideranalysis.model.ZomatoOrder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;

public class ZomatoOrderTimestampExtractor implements TimestampExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(ZomatoOrderTimestampExtractor.class);

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        try {
            ZomatoOrder zomatoOrder = (ZomatoOrder) record.value();
            return (zomatoOrder != null && zomatoOrder.getOrderDate() != null) ?
                    zomatoOrder.getOrderDate().toInstant(ZoneOffset.UTC).toEpochMilli() :
                    record.timestamp();
        } catch (Exception e) {
            LOG.error("Error extracting timestamp", e);
            return record.timestamp();
        }
    }
}
