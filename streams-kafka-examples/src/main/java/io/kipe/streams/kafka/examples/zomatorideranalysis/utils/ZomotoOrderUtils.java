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
package io.kipe.streams.kafka.examples.zomatorideranalysis.utils;

import io.kipe.streams.kafka.examples.zomatorideranalysis.model.ZomatoOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneOffset;

public class ZomotoOrderUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ZomotoOrderUtils.class);

    // Calculates the delivery speed for a given ZomatoOrder
    public static double calculateDeliverySpeed(ZomatoOrder zomatoOrder) {
        try {
            // Check if the ZomatoOrder or the necessary timestamps are null
            if (zomatoOrder == null || zomatoOrder.getPickupTime() == null || zomatoOrder.getDeliveredTime() == null) {
                return -1;
            }

            // Check if the delivery time is greater than the pickup time
            if (zomatoOrder.getDeliveredTime().toEpochSecond(ZoneOffset.UTC) < zomatoOrder.getPickupTime().toEpochSecond(ZoneOffset.UTC)) {
                return -1;
            }

            // Calculate the delivery speed
            double distanceInMiles = zomatoOrder.getFirstMileDistance() + zomatoOrder.getLastMileDistance();
            long deliveryTimeInSeconds = Duration.between(zomatoOrder.getPickupTime(), zomatoOrder.getDeliveredTime()).getSeconds();

            // Add a check here to avoid division by zero
            if (deliveryTimeInSeconds == 0) {
                LOG.error("Delivery time is 0 for order id: {} ", zomatoOrder.getOrderId());
                return -1;
            }

            return (distanceInMiles / deliveryTimeInSeconds) * 3600;
        } catch (Exception e) {
            LOG.error("Error calculating delivery speed", e);
            return -1;
        }
    }

}
