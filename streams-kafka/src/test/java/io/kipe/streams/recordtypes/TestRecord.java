package io.kipe.streams.recordtypes;//package io.kipe.streams.recordtypes;

// ------------------------------------------------------------------------
// records
// ------------------------------------------------------------------------

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TestRecord class that contains a timestamp and a key.
 * <p>
 * It's used to test the JoinBuilder.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TestRecord {
    public long timestamp;
    public String key;
}
