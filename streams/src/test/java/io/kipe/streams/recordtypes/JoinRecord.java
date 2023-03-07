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
