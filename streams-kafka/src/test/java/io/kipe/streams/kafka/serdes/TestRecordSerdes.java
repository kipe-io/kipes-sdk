/*
 * Kipes SDK for Kafka - The High-Level Event Processing SDK.
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
package io.kipe.streams.kafka.serdes;

import io.kipe.streams.kafka.serdes.deserializer.TestRecordDeserializer;
import io.kipe.streams.kafka.serdes.serializer.JsonSerializer;
import io.kipe.streams.recordtypes.TestRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class TestRecordSerdes implements Serde<TestRecord> {
    private final JsonSerializer<TestRecord> serializer = new JsonSerializer<>();
    private final TestRecordDeserializer<TestRecord> deserializer = new TestRecordDeserializer<>();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<TestRecord> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<TestRecord> deserializer() {
        return deserializer;
    }
}