package io.kipe.streams.kafka.processors.expressions.stats;

import io.kipe.streams.kafka.processors.StatsExpression;

/**
 * Stats expression to sum values of records.
 * <p>
 * This class provides a singleton instance of the Sum expression and a default field name "sum"
 * which will be used to store the sum value in the resulting record.
 * <p>
 * The valueFunction provided in the constructor takes in a key and value and returns the sum value
 * by first attempting to retrieve the count value from the provided field name in the value,
 * and if it is not present, it defaults to 0.
 * <p>
 * The class also provides a static factory method sum(..) to retrieve an instance.
 */
public class Sum extends StatsExpression {

	public static final String DEFAULT_FIELD = "sum";

	/**
	 * Returns the singleton instance of this class
	 *
	 * @param fieldNameToSum the field to sum the values of
	 * @return Sum singleton instance
	 */
	public static Sum sum(String fieldNameToSum) {
		return new Sum(fieldNameToSum);
	}

	private final String fieldNameToSum;
	
	/**
	 * Constructor for Sum class, which calls the constructor of the parent class {@link StatsExpression}
	 * with the default field name "sum". It also sets the valueFunction in the constructor.
	 */
	private Sum(String fieldNameToSum) {
		super(DEFAULT_FIELD);
		this.fieldNameToSum = fieldNameToSum;
		this.statsFunction = (groupKey, value, aggregate) -> {
			var sum = aggregate.getNumber(this.fieldName);
			var fieldValue = value.getNumber(this.fieldNameToSum);
			return sum == null? fieldValue.doubleValue() : sum.doubleValue() + fieldValue.doubleValue();
		};
	}

}
