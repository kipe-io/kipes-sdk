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

import io.kipe.streams.kafka.processors.AbstractGenericRecordProcessorTopologyTest;
import io.kipe.streams.kafka.processors.KipesBuilder;
import io.kipe.streams.kafka.processors.StatsBuilder;
import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.TopologyTestContext;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests {@link StatsBuilder} with Mode stats.
 */
class StatsBuilderModeTest extends AbstractGenericRecordProcessorTopologyTest {
    public StatsBuilderModeTest() {
        super(Map.of());
    }

    /**
     * Adds the stats processor to the topology builder, calculates the mode of records in each group.
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
                .with(Mode.mode("field")).as("myMode")
                .groupBy("group")
                .build(topologyTestContext.getJsonSerdeRegistry().getSerde(String.class));
    }

    /**
     * Tests the functionality of the stats processor.
     */
    @Test
    void test() {
        // given four records
        send(GenericRecord.create().with("group", "A").with("field", "apple"));
        send(GenericRecord.create().with("group", "A").with("field", "banana"));
        send(GenericRecord.create().with("group", "A").with("field", "apple"));
        send(GenericRecord.create().with("group", "B").with("field", "orange"));

        // then we get four results
        assertEquals(4, this.targetTopic.getQueueSize());

        GenericRecord r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(Set.of("apple"), r.getSet("myMode"));

        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(Set.of("apple", "banana"), r.getSet("myMode"));

        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(Set.of("apple"), r.getSet("myMode"));

        r = this.targetTopic.readValue();
        assertEquals("B", r.getString("group"));
        assertEquals(Set.of("orange"), r.getSet("myMode"));
    }

    @Test
    void testNullValuesInMiddle() {
        send(GenericRecord.create().with("group", "C").with("field", "apple"));
        send(GenericRecord.create().with("group", "C").with("field", null));
        send(GenericRecord.create().with("group", "C").with("field", "apple"));

        assertEquals(3, this.targetTopic.getQueueSize());

        GenericRecord r = this.targetTopic.readValue();
        assertEquals("C", r.getString("group"));
        assertEquals(Set.of("apple"), r.getSet("myMode"));

        r = this.targetTopic.readValue();
        assertEquals("C", r.getString("group"));
        assertEquals(Set.of("apple"), r.getSet("myMode"));

        r = this.targetTopic.readValue();
        assertEquals("C", r.getString("group"));
        assertEquals(Set.of("apple"), r.getSet("myMode"));
    }

    @Test
    void testNullValuesAtStart() {
        send(GenericRecord.create().with("group", "D").with("field", null));
        send(GenericRecord.create().with("group", "D").with("field", "banana"));

        assertEquals(2, this.targetTopic.getQueueSize());

        GenericRecord r = this.targetTopic.readValue();
        assertEquals("D", r.getString("group"));
        assertNull(r.get("myMode"));

        r = this.targetTopic.readValue();
        assertEquals("D", r.getString("group"));
        assertEquals(Set.of("banana"), r.getSet("myMode"));
    }
}
