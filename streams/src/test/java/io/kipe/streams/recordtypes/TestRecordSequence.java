package io.kipe.streams.recordtypes;

// ------------------------------------------------------------------------
// records
// ------------------------------------------------------------------------

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data class for the test records.
 * <p>
 * It's used to test the SequenceBuilder.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TestRecordSequence {
    public long timestamp;
    public String key;
    public Integer value;
    public Integer count;
}
