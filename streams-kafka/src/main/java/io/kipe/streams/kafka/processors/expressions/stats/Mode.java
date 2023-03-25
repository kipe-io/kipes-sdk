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

import io.kipe.streams.kafka.processors.StatsExpression;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class Mode extends StatsExpression {

    public static final String DEFAULT_FIELD = "mode";
    public static final String DEFAULT_FREQUENCY_MAP_FIELD = "frequencyMap";

    public static Mode mode(String fieldNameToMode) {
        return new Mode(fieldNameToMode);
    }

    private final String fieldNameToMode;

    private Mode(String fieldNameToMode) {
        super(DEFAULT_FIELD);
        this.fieldNameToMode = fieldNameToMode;
        this.statsFunction = (groupKey, value, aggregate) -> {
            Map<String, Integer> frequencyMap = aggregate.getMap(DEFAULT_FREQUENCY_MAP_FIELD);
            if (frequencyMap == null) {
                frequencyMap = new HashMap<>();
                aggregate.set(DEFAULT_FREQUENCY_MAP_FIELD, frequencyMap);
            }

            String fieldValue = value.getString(this.fieldNameToMode);
            frequencyMap.put(fieldValue, frequencyMap.getOrDefault(fieldValue, 0) + 1);

            int maxCount = frequencyMap.values().stream().max(Integer::compareTo).orElse(0);
            Set<String> modes = frequencyMap.entrySet().stream()
                    .filter(entry -> entry.getValue() == maxCount)
                    .map(Entry::getKey)
                    .collect(Collectors.toSet());

            return modes;
        };
    }

}
