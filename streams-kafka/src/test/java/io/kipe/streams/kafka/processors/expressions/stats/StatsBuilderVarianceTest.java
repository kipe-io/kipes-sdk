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
package io.kipe.streams.kafka.processors.expressions.stats;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kipe.streams.kafka.processors.AbstractGenericRecordProcessorTopologyTest;
import io.kipe.streams.kafka.processors.KipesBuilder;
import io.kipe.streams.kafka.processors.StatsBuilder;
import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.TopologyTestContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests {@link StatsBuilder} with Variance stats (var and varp).
 */
class StatsBuilderVarianceTest extends AbstractGenericRecordProcessorTopologyTest {
    public StatsBuilderVarianceTest() {
        super(Map.of());
    }

    /**
     * Adds the stats processor to the topology builder, calculates the variance of records in each group.
     *
     * @param builder             KipesBuilder<String, GenericRecord>
     * @param topologyTestContext TopologyTestContext
     * @return KipesBuilder<String, GenericRecord>
     */
    @Override
    protected KipesBuilder<String, GenericRecord> addGenericRecordProcessor(
            KipesBuilder<String, GenericRecord> builder,
            TopologyTestContext topologyTestContext) {
        return builder.stats()
                .with(Variance.var("field")).as("myVar")
                .with(Variance.varp("field")).as("myVarp")
                .groupBy("group")
                .build(topologyTestContext.getJsonSerdeRegistry().getSerde(String.class));
    }

    /**
     * Tests the functionality of the stats processor.
     */
    @Test
    void test() {
        send(GenericRecord.create().with("group", "A").with("field", 10));
        send(GenericRecord.create().with("group", "A").with("field", -20));
        send(GenericRecord.create().with("group", "B").with("field", 13));
        send(GenericRecord.create().with("group", "B").with("field", 25));

        assertEquals(4, this.targetTopic.getQueueSize());

        GenericRecord r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(0.0, r.getNumber("myVar").doubleValue());
        assertEquals(0.0, r.getNumber("myVarp").doubleValue());

        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(450.0, r.getNumber("myVar").doubleValue());
        assertEquals(225.0, r.getNumber("myVarp").doubleValue());

        r = this.targetTopic.readValue();
        assertEquals("B", r.getString("group"));
        assertEquals(0.0, r.getNumber("myVar").doubleValue());
        assertEquals(0.0, r.getNumber("myVarp").doubleValue());

        r = this.targetTopic.readValue();
        assertEquals("B", r.getString("group"));
        assertEquals(72.0, r.getNumber("myVar").doubleValue());
        assertEquals(36.0, r.getNumber("myVarp").doubleValue());
    }

    @Test
    void testSameValue() {
        send(GenericRecord.create().with("group", "C").with("field", 5));
        send(GenericRecord.create().with("group", "C").with("field", 5));
        send(GenericRecord.create().with("group", "C").with("field", 5));

        GenericRecord r = this.targetTopic.readValue();
        assertEquals("C", r.getString("group"));
        assertEquals(0.0, r.getNumber("myVar").doubleValue());
        assertEquals(0.0, r.getNumber("myVarp").doubleValue());

        r = this.targetTopic.readValue();
        assertEquals("C", r.getString("group"));
        assertEquals(0.0, r.getNumber("myVar").doubleValue());
        assertEquals(0.0, r.getNumber("myVarp").doubleValue());

        r = this.targetTopic.readValue();
        assertEquals("C", r.getString("group"));
        assertEquals(0.0, r.getNumber("myVar").doubleValue());
        assertEquals(0.0, r.getNumber("myVarp").doubleValue());
    }

    @Test
    void testNullValues() {
        send(GenericRecord.create().with("group", "D").with("field", 10));
        send(GenericRecord.create().with("group", "D").with("field", null));
        send(GenericRecord.create().with("group", "D").with("field", -20));
        send(GenericRecord.create().with("group", "E").with("field", null));
        send(GenericRecord.create().with("group", "E").with("field", -20));
        
        assertEquals(5, this.targetTopic.getQueueSize());

        GenericRecord r = this.targetTopic.readValue();
        assertEquals("D", r.getString("group"));
        assertEquals(0.0, r.getNumber("myVar").doubleValue());
        assertEquals(0.0, r.getNumber("myVarp").doubleValue());

        r = this.targetTopic.readValue();
        assertEquals("D", r.getString("group"));
        assertEquals(0.0, r.getNumber("myVar").doubleValue());
        assertEquals(0.0, r.getNumber("myVarp").doubleValue());

        r = this.targetTopic.readValue();
        assertEquals("D", r.getString("group"));
        assertEquals(450.0, r.getNumber("myVar").doubleValue());
        assertEquals    (225.0, r.getNumber("myVarp").doubleValue());

        r = this.targetTopic.readValue();
        assertEquals("E", r.getString("group"));
        assertNull(r.get("myVar"));
        assertNull(r.get("myVarp"));

        r = this.targetTopic.readValue();
        assertEquals("E", r.getString("group"));
        assertEquals(0.0, r.getNumber("myVar").doubleValue());
        assertEquals(0.0, r.getNumber("myVarp").doubleValue());
    }
}
