package de.tradingpulse.connector.iexcloud.service;

import static java.time.temporal.ChronoUnit.*;
import java.time.LocalDate;
import java.time.temporal.TemporalUnit;
import java.util.Optional;

public enum IEXCloudRange {
	MAX(15, YEARS, "max"),
	Y5(5, YEARS, "5y"),
	Y2(2, YEARS, "2y"),
	Y1(1, YEARS, "1y"),
	M6(6, MONTHS, "6m"),
	M3(3, MONTHS, "3m"),
	M1(1, MONTHS, "1m"),
	D5(5, DAYS, "5d");
	
	/**
	 * Returns the {@link IEXCloudRange} which covers the dateInPast and has the
	 * least excess days. 
	 * 
	 * @param dateInPast
	 * @return
	 */
	public static Optional<IEXCloudRange> findLeastExcessDaysRange(LocalDate dateInPast) {
		IEXCloudRange currentBestMatch = null;
		long currentLeastExcessDays = -1;
		
		for(IEXCloudRange currentRange : values()) {
			if(! currentRange.isCovered(dateInPast)) {
				continue;
			}
			
			long currentExcessDays = currentRange.getExcessDays(dateInPast);
			
			if(currentBestMatch == null || currentExcessDays < currentLeastExcessDays) {
				currentBestMatch = currentRange;
				currentLeastExcessDays = currentExcessDays;				
			}
		}
		
		return Optional.ofNullable(currentBestMatch);
	}
	
	
	private final long unitsCovered;
	private final TemporalUnit unit;
	private final String range;
	
	private IEXCloudRange(long unitsCovered, TemporalUnit unit, String range) {
		this.unitsCovered = unitsCovered;
		this.unit = unit;
		this.range = range;
	}
	
	public String getRange() {
		return this.range;
	}
	
	/**
	 * Returns whether this range includes the given dateInPast starting from
	 * today.
	 */
	public boolean isCovered(LocalDate dateInPast) {
		return isCovered(LocalDate.now(), dateInPast);
	}

	private boolean isCovered(LocalDate now, LocalDate dateInPast) {
		LocalDate earliestCovered = earliestCovered(now);
		return earliestCovered.isBefore(dateInPast) || earliestCovered.isEqual(dateInPast);
	}
	
	private LocalDate earliestCovered(LocalDate now) {
		return now.minus(this.unitsCovered, this.unit);
	}
	
	/**
	 * Returns the number of days from rangeStart to dateInPast.
	 * 
	 * @throws IllegalArgumentException if dateInPast is not covered by this 
	 * range.
	 */
	public long getExcessDays(LocalDate dateInPast) {
		LocalDate now = LocalDate.now();
		if(! isCovered(now, dateInPast)) {
			throw new IllegalArgumentException("dateInPast is not covered by this range.");
		}
		
		LocalDate earliestCovered = earliestCovered(now);
		return earliestCovered.until(dateInPast, DAYS);
	}
}
