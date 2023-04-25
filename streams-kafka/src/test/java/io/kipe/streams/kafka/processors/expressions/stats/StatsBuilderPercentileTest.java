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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests {@link StatsBuilder} with Percentile stats.
 */
class StatsBuilderPercentileTest extends AbstractGenericRecordProcessorTopologyTest {
    public StatsBuilderPercentileTest() {
        super(Map.of());
    }

    /**
     * Adds the stats processor to the topology builder, calculates the percentile of records in each group.
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
                .with(Percentile.median("field")).as("myMedian")
                .with(Percentile.perc5("field")).as("myPerc5")
                .with(Percentile.perc10("field")).as("myPerc10")
                .with(Percentile.perc15("field")).as("myPerc15")
                .with(Percentile.perc20("field")).as("myPerc20")
                .with(Percentile.perc25("field")).as("myPerc25")
                .with(Percentile.perc30("field")).as("myPerc30")
                .with(Percentile.perc35("field")).as("myPerc35")
                .with(Percentile.perc40("field")).as("myPerc40")
                .with(Percentile.perc45("field")).as("myPerc45")
                .with(Percentile.perc50("field")).as("myPerc50")
                .with(Percentile.perc55("field")).as("myPerc55")
                .with(Percentile.perc60("field")).as("myPerc60")
                .with(Percentile.perc65("field")).as("myPerc65")
                .with(Percentile.perc70("field")).as("myPerc70")
                .with(Percentile.perc75("field")).as("myPerc75")
                .with(Percentile.perc80("field")).as("myPerc80")
                .with(Percentile.perc85("field")).as("myPerc85")
                .with(Percentile.perc90("field")).as("myPerc90")
                .with(Percentile.perc95("field")).as("myPerc95")
                .groupBy("group")
                .build(topologyTestContext.getJsonSerdeRegistry().getSerde(String.class));
    }

    @Test
    void testPercentile() {
        send(GenericRecord.create().with("group", "A").with("field", 10.0));
        send(GenericRecord.create().with("group", "A").with("field", 20.0));
        send(GenericRecord.create().with("group", "A").with("field", 30.0));
        send(GenericRecord.create().with("group", "A").with("field", 40.0));
        send(GenericRecord.create().with("group", "A").with("field", 50.0));

        assertEquals(5, this.targetTopic.getQueueSize());

        GenericRecord r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(10.0, r.getDouble("myMedian"));
        assertEquals(10.0, r.getDouble("myPerc5"));
        assertEquals(10.0, r.getDouble("myPerc10"));
        assertEquals(10.0, r.getDouble("myPerc15"));
        assertEquals(10.0, r.getDouble("myPerc20"));
        assertEquals(10.0, r.getDouble("myPerc25"));
        assertEquals(10.0, r.getDouble("myPerc30"));
        assertEquals(10.0, r.getDouble("myPerc35"));
        assertEquals(10.0, r.getDouble("myPerc40"));
        assertEquals(10.0, r.getDouble("myPerc45"));
        assertEquals(10.0, r.getDouble("myPerc50"));
        assertEquals(10.0, r.getDouble("myPerc55"));
        assertEquals(10.0, r.getDouble("myPerc60"));
        assertEquals(10.0, r.getDouble("myPerc65"));
        assertEquals(10.0, r.getDouble("myPerc70"));
        assertEquals(10.0, r.getDouble("myPerc75"));
        assertEquals(10.0, r.getDouble("myPerc80"));
        assertEquals(10.0, r.getDouble("myPerc85"));
        assertEquals(10.0, r.getDouble("myPerc90"));
        assertEquals(10.0, r.getDouble("myPerc95"));

        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(15.0, r.getDouble("myMedian"));
        assertEquals(10.5, r.getDouble("myPerc5"));
        assertEquals(11.0, r.getDouble("myPerc10"));
        assertEquals(11.5, r.getDouble("myPerc15"));
        assertEquals(12.0, r.getDouble("myPerc20"));
        assertEquals(12.5, r.getDouble("myPerc25"));
        assertEquals(13.0, r.getDouble("myPerc30"));
        assertEquals(13.5, r.getDouble("myPerc35"));
        assertEquals(14.0, r.getDouble("myPerc40"));
        assertEquals(14.5, r.getDouble("myPerc45"));
        assertEquals(15.0, r.getDouble("myPerc50"));
        assertEquals(15.5, r.getDouble("myPerc55"));
        assertEquals(16.0, r.getDouble("myPerc60"));
        assertEquals(16.5, r.getDouble("myPerc65"));
        assertEquals(17.0, r.getDouble("myPerc70"));
        assertEquals(17.5, r.getDouble("myPerc75"));
        assertEquals(18.0, r.getDouble("myPerc80"));
        assertEquals(18.5, r.getDouble("myPerc85"));
        assertEquals(19.0, r.getDouble("myPerc90"));
        assertEquals(19.5, r.getDouble("myPerc95"));

        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(20.0, r.getDouble("myMedian"));
        assertEquals(11.0, r.getDouble("myPerc5"));
        assertEquals(12.0, r.getDouble("myPerc10"));
        assertEquals(13.0, r.getDouble("myPerc15"));
        assertEquals(14.0, r.getDouble("myPerc20"));
        assertEquals(15.0, r.getDouble("myPerc25"));
        assertEquals(16.0, r.getDouble("myPerc30"));
        assertEquals(17.0, r.getDouble("myPerc35"));
        assertEquals(18.0, r.getDouble("myPerc40"));
        assertEquals(19.0, r.getDouble("myPerc45"));
        assertEquals(20.0, r.getDouble("myPerc50"));
        assertEquals(21.0, r.getDouble("myPerc55"));
        assertEquals(22.0, r.getDouble("myPerc60"));
        assertEquals(23.0, r.getDouble("myPerc65"));
        assertEquals(24.0, r.getDouble("myPerc70"));
        assertEquals(25.0, r.getDouble("myPerc75"));
        assertEquals(26.0, r.getDouble("myPerc80"));
        assertEquals(27.0, r.getDouble("myPerc85"));
        assertEquals(28.0, r.getDouble("myPerc90"));
        assertEquals(29.0, r.getDouble("myPerc95"));

        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(25.0, r.getDouble("myMedian"));
        assertEquals(11.5, r.getDouble("myPerc5"));
        assertEquals(13.0, r.getDouble("myPerc10"));
        assertEquals(14.5, r.getDouble("myPerc15"));
        assertEquals(16.0, r.getDouble("myPerc20"));
        assertEquals(17.5, r.getDouble("myPerc25"));
        assertEquals(19.0, r.getDouble("myPerc30"));
        assertEquals(20.5, r.getDouble("myPerc35"));
        assertEquals(22.0, r.getDouble("myPerc40"));
        assertEquals(23.5, r.getDouble("myPerc45"));
        assertEquals(25.0, r.getDouble("myPerc50"));
        assertEquals(26.5, r.getDouble("myPerc55"), 0.01);
        assertEquals(28.0, r.getDouble("myPerc60"));
        assertEquals(29.5, r.getDouble("myPerc65"));
        assertEquals(31.0, r.getDouble("myPerc70"), 0.01);
        assertEquals(32.5, r.getDouble("myPerc75"));
        assertEquals(34.0, r.getDouble("myPerc80"));
        assertEquals(35.5, r.getDouble("myPerc85"));
        assertEquals(37.0, r.getDouble("myPerc90"));
        assertEquals(38.5, r.getDouble("myPerc95"));

        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(30.0, r.getDouble("myMedian"));
        assertEquals(12.0, r.getDouble("myPerc5"));
        assertEquals(14.0, r.getDouble("myPerc10"));
        assertEquals(16.0, r.getDouble("myPerc15"));
        assertEquals(18.0, r.getDouble("myPerc20"));
        assertEquals(20.0, r.getDouble("myPerc25"));
        assertEquals(22.0, r.getDouble("myPerc30"));
        assertEquals(24.0, r.getDouble("myPerc35"));
        assertEquals(26.0, r.getDouble("myPerc40"));
        assertEquals(28.0, r.getDouble("myPerc45"));
        assertEquals(30.0, r.getDouble("myPerc50"));
        assertEquals(32.0, r.getDouble("myPerc55"));
        assertEquals(34.0, r.getDouble("myPerc60"));
        assertEquals(36.0, r.getDouble("myPerc65"));
        assertEquals(38.0, r.getDouble("myPerc70"));
        assertEquals(40.0, r.getDouble("myPerc75"));
        assertEquals(42.0, r.getDouble("myPerc80"));
        assertEquals(44.0, r.getDouble("myPerc85"));
        assertEquals(46.0, r.getDouble("myPerc90"));
        assertEquals(48.0, r.getDouble("myPerc95"));
    }

    @Test
    void testNullValuesInMiddle() {
        send(GenericRecord.create().with("group", "A").with("field", 10.0));
        send(GenericRecord.create().with("group", "A").with("field", 20.0));
        send(GenericRecord.create().with("group", "A").with("field", null));
        send(GenericRecord.create().with("group", "B").with("field", 30.0));
        send(GenericRecord.create().with("group", "B").with("field", 40.0));

        assertEquals(5, this.targetTopic.getQueueSize());

        GenericRecord r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(10.0, r.getDouble("myMedian"));

        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(15.0, r.getDouble("myMedian"));

        r = this.targetTopic.readValue();
        assertEquals("A", r.getString("group"));
        assertEquals(15.0, r.getDouble("myMedian"));

        r = this.targetTopic.readValue();
        assertEquals("B", r.getString("group"));
        assertEquals(30.0, r.getDouble("myMedian"));

        r = this.targetTopic.readValue();
        assertEquals("B", r.getString("group"));
        assertEquals(35.0, r.getDouble("myMedian"));
    }

    @Test
    void testNullValuesAtStart() {
        send(GenericRecord.create().with("group", "C").with("field", null));
        send(GenericRecord.create().with("group", "C").with("field", 15.0));

        assertEquals(2, this.targetTopic.getQueueSize());

        GenericRecord r = this.targetTopic.readValue();
        assertEquals("C", r.getString("group"));
        assertNull(r.getNumber("myMedian"));

        r = this.targetTopic.readValue();
        assertEquals("C", r.getString("group"));
        assertEquals(15.0, r.getDouble("myMedian"));
    }

    @Test
    void testDuplicateRecords() {
        send(GenericRecord.create().with("group", "D").with("field", 10.0));
        send(GenericRecord.create().with("group", "D").with("field", 10.0));
        send(GenericRecord.create().with("group", "D").with("field", 20.0));

        assertEquals(3, this.targetTopic.getQueueSize());

        GenericRecord r = this.targetTopic.readValue();
        assertEquals("D", r.getString("group"));
        assertEquals(10.0, r.getDouble("myPerc5"));
        assertEquals(10.0, r.getDouble("myPerc25"));
        assertEquals(10.0, r.getDouble("myPerc50"));
        assertEquals(10.0, r.getDouble("myPerc75"));
        assertEquals(10.0, r.getDouble("myPerc95"));

        r = this.targetTopic.readValue();
        assertEquals("D", r.getString("group"));
        assertEquals(10.0, r.getDouble("myPerc5"));
        assertEquals(10.0, r.getDouble("myPerc25"));
        assertEquals(10.0, r.getDouble("myPerc50"));
        assertEquals(10.0, r.getDouble("myPerc75"));
        assertEquals(10.0, r.getDouble("myPerc95"));

        r = this.targetTopic.readValue();
        assertEquals("D", r.getString("group"));
        assertEquals(10.0, r.getDouble("myPerc5"));
        assertEquals(10.0, r.getDouble("myPerc25"));
        assertEquals(10.0, r.getDouble("myPerc50"));
        assertEquals(15.0, r.getDouble("myPerc75"));
        assertEquals(19.0, r.getDouble("myPerc95"));
    }

}
