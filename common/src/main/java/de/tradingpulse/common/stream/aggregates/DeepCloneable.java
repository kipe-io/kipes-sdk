package de.tradingpulse.common.stream.aggregates;

/**
 * Interface for classes providing deep clones of itself. A deep clone has all
 * internal references cloned.
 * 
 * @param <A>
 */
public interface DeepCloneable <A>{

	A deepClone();
}
