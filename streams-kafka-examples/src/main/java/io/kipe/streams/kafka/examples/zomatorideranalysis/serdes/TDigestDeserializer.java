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
package io.kipe.streams.kafka.examples.zomatorideranalysis.serdes;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;

public class TDigestDeserializer implements Deserializer<TDigest> {

    @Override
    public TDigest deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);
            return MergingDigest.fromBytes(buffer);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing TDigest", e);
        }
    }
}

