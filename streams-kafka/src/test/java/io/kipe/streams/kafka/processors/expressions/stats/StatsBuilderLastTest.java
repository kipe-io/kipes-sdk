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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kipe.streams.kafka.processors.AbstractGenericRecordProcessorTopologyTest;
import io.kipe.streams.kafka.processors.KipesBuilder;
import io.kipe.streams.kafka.processors.StatsBuilder;
import io.kipe.streams.recordtypes.GenericRecord;
import io.kipe.streams.test.kafka.TopologyTestContext;

/**
 * This class tests the functionality of {@link StatsBuilder} in conjunction with the Last stats.
 */
class StatsBuilderLastTest extends AbstractGenericRecordProcessorTopologyTest {
    public StatsBuilderLastTest() {
        super(Map.of());
    }

    /**
     * This method is used to add the stats processor to the topology builder.
     * It uses the Last.last() method to get the last value of the field 'field' in each group.
     * It groups the records by 'group' field and returns the topology builder.
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
                .with(Last.last("field")).as("myLast")
                .groupBy("group")
                .build(topologyTestContext.getJsonSerdeRegistry().getSerde(String.class));
    }

    /**
     * This is the test method that asserts the functionality of the stats processor.
     */
    @Test
    void test() {
        // given three records
        send(GenericRecord.create().with("group", "A").with("field", 10));
        send(GenericRecord.create().with("group", "A").with("field", -20));
        send(GenericRecord.create().with("group", "B").with("field", 13));

        // then we get three results
        assertEquals(3, this.targetTopic.getQueueSize());

        GenericRecord r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(10, r.getNumber("myLast").intValue());

        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(-20, r.getNumber("myLast").intValue());

        r = this.targetTopic.readValue();
        assertEquals("B", r.getString("group"));
        assertEquals(13, r.getNumber("myLast").intValue());
    }

    @Test
    void testNullValues() {
        send(GenericRecord.create().with("group", "A").with("field", null));
        send(GenericRecord.create().with("group", "A").with("field", 10));
        send(GenericRecord.create().with("group", "A").with("field", null));
        send(GenericRecord.create().with("group", "A").with("field", -20));
        send(GenericRecord.create().with("group", "B").with("field", 13));

        assertEquals(5, this.targetTopic.getQueueSize());
        
        GenericRecord r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertNull(r.getNumber("myLast"));

        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(10, r.getNumber("myLast"));
        
        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(10, r.getNumber("myLast"));
        
        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(-20, r.getNumber("myLast").intValue());

        r = this.targetTopic.readValue();
        assertEquals("B", r.getString("group"));
        assertEquals(13, r.getNumber("myLast").intValue());
    }
}
