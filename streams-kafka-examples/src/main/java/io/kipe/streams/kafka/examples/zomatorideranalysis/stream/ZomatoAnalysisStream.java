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

import com.tdunning.math.stats.TDigest;
import io.kipe.streams.kafka.examples.zomatorideranalysis.model.CompositeKey;
import io.kipe.streams.kafka.examples.zomatorideranalysis.model.DeliverySpeed;
import io.kipe.streams.kafka.examples.zomatorideranalysis.model.PercenitleEvent;
import io.kipe.streams.kafka.examples.zomatorideranalysis.model.ZomatoOrder;
import io.kipe.streams.kafka.examples.zomatorideranalysis.serdes.TDigestSerde;
import io.kipe.streams.kafka.examples.zomatorideranalysis.utils.ZomotoOrderUtils;
import io.kipe.streams.kafka.factories.JsonSerdeFactory;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;

@Factory
public class ZomatoAnalysisStream {
    private static final Logger LOG = LoggerFactory.getLogger(ZomatoAnalysisStream.class);
    private static final String ORDER_TOPIC = "zomato.orders";
    private static final String DELIVERY_SPEED_TOPIC = "zomato.delivery.speed";
    private static final String GLOBAL_PERCENTILE_SPEEDS_TOPIC = "zomato.global.percentile.speeds";
    private static final String WORMHOLERS_TOPIC = "zomato.wormholers";
    private static final String RIDER_DAILY_SPEEDS_STORE = "zomato.rider.daily.speeds";
    private static final String RIDER_DAILY_SPEED_PERCENTILES_STORE = "zomato.rider.daily.speed.percentiles";

    // The main stream processing topology
    @Singleton
    public Topology zomatoAnalysisStream(ConfiguredStreamBuilder builder) {
        // Define the Serdes used in this topology
        Serde<Long> longSerde = Serdes.Long();
        Serde<Double> doubleSerde = Serdes.Double();
        Serde<ZomatoOrder> zomatoOrderSerde = JsonSerdeFactory.getJsonSerde(ZomatoOrder.class);
        Serde<DeliverySpeed> deliverySpeedSerde = JsonSerdeFactory.getJsonSerde(DeliverySpeed.class);
        Serde<PercenitleEvent> percenitleEventSerde = JsonSerdeFactory.getJsonSerde(PercenitleEvent.class);
        Serde<LocalDate> localDateSerde = JsonSerdeFactory.getJsonSerde(LocalDate.class);
        Serde<CompositeKey> compositeKeySerde = JsonSerdeFactory.getJsonSerde(CompositeKey.class);
        Serde<Windowed<LocalDate>> localDateWindowSerde = new WindowedSerdes.TimeWindowedSerde(localDateSerde, 100000);

        // Create a stream of ZomatoOrder from the topic
        KStream<Long, ZomatoOrder> zomatoOrderStream = builder.stream(
                ORDER_TOPIC,
                Consumed
                        .with(longSerde, zomatoOrderSerde)
                        .withTimestampExtractor(new ZomatoOrderTimestampExtractor())
        );

        // Calculate delivery speed per mile and filter out invalid speeds
        KStream<Long, DeliverySpeed> deliverySpeedStream = zomatoOrderStream
                .mapValues(zomatoOrder -> new DeliverySpeed(
                        zomatoOrder.getOrderDate().toLocalDate(),
                        zomatoOrder.getOrderId(),
                        zomatoOrder.getRiderId(),
                        ZomotoOrderUtils.calculateDeliverySpeed(zomatoOrder)
                ))
                .filter((orderId, speed) -> speed.getSpeed() > 0)
//                .peek((orderId, deliverySpeed) -> LOG.info("orderId={} deliverySpeed={}", orderId, deliverySpeed))
                ;

        // Write the delivery speeds to a separate topic
        deliverySpeedStream.to(DELIVERY_SPEED_TOPIC, Produced.with(longSerde, deliverySpeedSerde));

        // Create daily speed percentile aggregates

        KTable<Windowed<LocalDate>, TDigest> dailySpeedPercentilesDigest = deliverySpeedStream
                .map((k, v) -> new KeyValue<>(v.getDate(), v.getSpeed()))
                .groupByKey(Grouped.with(localDateSerde, doubleSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)))
                .aggregate(
                        () -> TDigest.createDigest(100),
                        (key, speed, digest) -> {
                            digest.add(speed);
                            return digest;
                        },
                        Materialized.<LocalDate, TDigest, WindowStore<Bytes, byte[]>>as(RIDER_DAILY_SPEEDS_STORE)
                                .withKeySerde(localDateSerde)
                                .withValueSerde(new TDigestSerde())
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        // Calculate a global percentile for each day.
        KTable<Windowed<LocalDate>, Double> daily80thPercentile = dailySpeedPercentilesDigest
                .mapValues((key, digest) -> {
                            Double percentile = digest.quantile(0.8);
                            LOG.info("Global 80th percentile for window {} is {}", key, percentile);
                            return percentile;
                        }
                );

        // Write global 80th percentile speeds to a new Kafka topic
        daily80thPercentile
                .toStream()
                .peek((key, value) -> LOG.info("GLOBAL Window start time={}, 80th Percentile speed={}", key.window().startTime(), value))
                .to(GLOBAL_PERCENTILE_SPEEDS_TOPIC, Produced.with(localDateWindowSerde, doubleSerde));

        // Calculate 80th percentile speed for each rider for each day
        KTable<Windowed<CompositeKey>, Double> riderDaily80thPercentileSpeed = deliverySpeedStream

                .map((orderId, deliverySpeed) -> new KeyValue<>(new CompositeKey(deliverySpeed.getDate(), deliverySpeed.getRiderId()), deliverySpeed.getSpeed()))
                .groupByKey(Grouped.with(compositeKeySerde, doubleSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1)))
                .aggregate(
                        () -> TDigest.createDigest(100),
                        (key, speed, digest) -> {
                            digest.add(speed);
                            return digest;
                        },
                        Materialized.<CompositeKey, TDigest, WindowStore<Bytes, byte[]>>as(RIDER_DAILY_SPEED_PERCENTILES_STORE)
                                .withKeySerde(compositeKeySerde)
                                .withValueSerde(new TDigestSerde())
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .mapValues((key, digest) -> {
                    Double percentile = digest.quantile(0.8);
                    LOG.info("Rider specific 80th percentile for window {} is {}", key, percentile);
                    return percentile;
                });

        // Create the stream for the global percentile
        KStream<Windowed<LocalDate>, Double> globalPerc80Stream = builder.stream(
                GLOBAL_PERCENTILE_SPEEDS_TOPIC,
                Consumed.with(localDateWindowSerde, Serdes.Double()));

        // Create the stream for the rider-specific percentile
        KStream<Windowed<CompositeKey>, Double> riderDaily80thPercentileStream = riderDaily80thPercentileSpeed.toStream();

        // Adjust the key of the rider-specific stream to match the key of the global stream
        KStream<Windowed<LocalDate>, PercenitleEvent> riderDaily80thPercentileStreamAdjusted = riderDaily80thPercentileStream
                .map((key, value) -> KeyValue.pair(new Windowed<>(key.key().getDate(), key.window()), new PercenitleEvent(key.key().getDate(), key.key().getRiderId(), value)));

        KTable<Windowed<LocalDate>, Double> globalPerc80Table = globalPerc80Stream.toTable(Materialized.with(localDateWindowSerde, doubleSerde));

        KStream<Windowed<LocalDate>, PercenitleEvent> joinedStream = riderDaily80thPercentileStreamAdjusted.join(
                        globalPerc80Table,
                        (key, riderSpeed, globalSpeed) -> {
                            LOG.info("Joining rider 80th percentile {} with global 80th percentile {} for window {}",
                                    riderSpeed.getSpeed(), globalSpeed, key);
                            return new PercenitleEvent(
                                    riderSpeed.getDate(),
                                    riderSpeed.getRiderId(),
                                    (globalSpeed != null && riderSpeed.getSpeed() > globalSpeed) ? riderSpeed.getSpeed() : 0.0
                            );
                        },
                        Joined.with(localDateWindowSerde, percenitleEventSerde, Serdes.Double())
                )
                .filter((date, deliverySpeed) -> deliverySpeed.getSpeed() > 0)
                .peek((date, deliverySpeed) -> LOG.info("Wormholer detected ::: date={}, riderId={}, deliverySpeed={}", deliverySpeed.getDate(), deliverySpeed.getRiderId(), deliverySpeed));

        joinedStream.to(WORMHOLERS_TOPIC, Produced.with(localDateWindowSerde, percenitleEventSerde));

        return builder.build();
    }
}
