package de.tradingpulse.optimus.analytics;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;


public abstract class CollectingFilter <T>  implements Predicate<T> {

	private final Set<T> passingObjects = new HashSet<>();

	public Set<T> getPassingObjects() {
		return Collections.unmodifiableSet(passingObjects);
	}
	
	public void clear() {
		passingObjects.clear();
	}

	@Override
	public synchronized boolean test(T t) {
		boolean passes = doTest(t);
		
		if(passes) {
			passingObjects.add(t);
		}
		
		return passes;
	}
	
	protected abstract boolean doTest(T t);
}
