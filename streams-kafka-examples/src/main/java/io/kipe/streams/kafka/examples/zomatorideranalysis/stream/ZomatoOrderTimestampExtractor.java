/*
 * Kipes SDK Examples - The High-Level Event Processing SDK.
 * Copyright © 2023 kipe.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
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
