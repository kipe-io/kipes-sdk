/*
 * Kipe Streams Kafka - Kipe Streams SDK
 * Copyright © 2023 Kipe.io
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
package io.kipe.streams.recordtypes;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * JoinRecord class that contains a timestamp, a key, and two TestRecord objects.
 * <p>
 * It also contains a method 'from' to create a new JoinRecord from two TestRecord objects.
 * <p>
 * It's used to test the JoinBuilder.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class JoinRecord {
    public static JoinRecord from(TestRecord left, TestRecord right) {
        return new JoinRecord(
                Math.max(left.timestamp, right.timestamp),
                left.key,
                left,
                right);
    }

    long timestamp;
    String key;

    TestRecord left;
    TestRecord right;
}
