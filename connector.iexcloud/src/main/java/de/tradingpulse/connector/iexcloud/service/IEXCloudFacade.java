package de.tradingpulse.connector.iexcloud.service;

import static java.time.DayOfWeek.FRIDAY;
import static java.time.DayOfWeek.SATURDAY;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class IEXCloudFacade {

	private static final Logger LOG = LoggerFactory.getLogger(IEXCloudFacade.class);
	
	private final Password apiToken;
	private final Password apiSecret;
	private final int initialTimerangeInDays;
	private final IEXCloudService iexCloudService;
	
	public IEXCloudFacade(
			final String baseUrl, 
			final Password apiToken, 
			final Password apiSecret,
			final int initialTimerangeInDays )
	{
		this(apiToken, apiSecret, initialTimerangeInDays, createIexCloudService(baseUrl));
	}
	
	private static IEXCloudService createIexCloudService(final String baseUrl) {
		return new Retrofit.Builder()
				.baseUrl(baseUrl)
				.addConverterFactory(JacksonConverterFactory.create())
				.build()
				.create(IEXCloudService.class);
	}

	IEXCloudFacade(
			final Password apiToken,
			final Password apiSecret,
			final int initialTimerangeInDays,
			IEXCloudService iexCloudService)
	{
		this.apiToken = apiToken;
		this.apiSecret = apiSecret;
		this.initialTimerangeInDays = initialTimerangeInDays;
		this.iexCloudService = iexCloudService;
	}

	public IEXCloudMetadata fetchMetadata() {
		try {
			return iexCloudService.fetchMetadata(this.apiSecret.value()).execute().body();
		} catch (IOException e) {
			LOG.error("exception during fetchMetadata, going to return null", e);
			
			return null;
		}
	}
	
	public List<IEXCloudOHLCVRecord> fetchOHLCVSince(String symbol, LocalDate lastFetchedDate) throws NoRecordsProvidedException {
		return internalFetchOHLCVSince(symbol, lastFetchedDate, LocalDate.now());
	}
	
	// method exists only for testing purposes
	List<IEXCloudOHLCVRecord> internalFetchOHLCVSince(String symbol, LocalDate lastFetchedDate, LocalDate todayDate) throws NoRecordsProvidedException {
		// TODO a smarter implementation would consider the exchange's timezone the symbol is traded at
		// We are just working on LocalDate which will lead to situations where
		// we are going to fetch the same dates multiple times
		
		LOG.debug("{}/{}: evaluating need to fetch for lastFetchedDate {}", symbol, todayDate, lastFetchedDate);
		
		// TODO make first fetch range configurable
		LocalDate fetchStartDate = lastFetchedDate == null? todayDate.minusDays(this.initialTimerangeInDays) : lastFetchedDate.plusDays(1);
		DayOfWeek fetchStartDay = fetchStartDate.getDayOfWeek();

		// return an empty list if
		// - fetchStartDate is >= today
		// - fetchStartDate is a Saturday and today is Sunday, or Monday
		if(fetchStartDate.isEqual(todayDate)
				|| fetchStartDate.isAfter(todayDate)
				|| (fetchStartDay == SATURDAY) && fetchStartDate.until(todayDate, ChronoUnit.DAYS) <= 2) {
			
			LOG.debug("{}/{}: no need to fetch starting from Śat, {}", symbol, todayDate, fetchStartDate);
			
			return Collections.emptyList();
		}
		
		// fetch previous day if
		// - fetchStartDate is yesterday
		// - fetchStartDate is a Friday and today is Saturday, Sunday, or Monday
		if(fetchStartDate.isEqual(todayDate.minusDays(1)) 
				|| (fetchStartDay == FRIDAY) && fetchStartDate.until(todayDate, ChronoUnit.DAYS) <= 3) {

			LOG.debug("{}/{}: fetch previous records for Fri, {}", symbol, todayDate, fetchStartDate);

			return removeAlreadyFetchedDates(Arrays.asList(fetchOHLCVPrevious(symbol)), lastFetchedDate);
		}
		
		// in all other cases get the range with least excess days and fetch
		// that range and remove already fetched dates
		Optional<IEXCloudRange> optRange = IEXCloudRange.findLeastExcessDaysRange(fetchStartDate);
		if(optRange.isEmpty()) {
			
			LOG.warn("{}/{}: couldn't find a matching range for {}", symbol, todayDate, fetchStartDate);
			return Collections.emptyList();
		}
		
		LOG.debug("{}/{}: going to fetch with range {} for {}", symbol, todayDate, optRange.get(), fetchStartDate);
		
		return removeAlreadyFetchedDates(fetchOHLCVRange(symbol, optRange.get()), lastFetchedDate);
	}

	List<IEXCloudOHLCVRecord> removeAlreadyFetchedDates(List<IEXCloudOHLCVRecord> records, LocalDate lastFetchedDate) {
		
		return records.stream()
				.filter(Objects::nonNull)
				.filter(record -> lastFetchedDate == null? true : record.getLocalDate().isAfter(lastFetchedDate))
				.collect(Collectors.toList());
	}
	
	List<IEXCloudOHLCVRecord> fetchOHLCVRange(String symbol, IEXCloudRange range) throws NoRecordsProvidedException {
		try {
			return fetchAdaptiveOHLCVRange(symbol, range);
		} catch (IOException e) {
			LOG.error(
					String.format("exception during fetchOHLCVRange for symbol '%s' and range '%s', going to return empty list", symbol, range.getRange()),
					e);
			return Collections.emptyList();
		}
	}
	
	private List<IEXCloudOHLCVRecord> fetchAdaptiveOHLCVRange(String symbol, IEXCloudRange range) throws IOException, NoRecordsProvidedException {
		List<IEXCloudOHLCVRecord> records;
		IEXCloudRange currentRange = range;
		
		do {
			// TODO: check for HTTP errors
			Response<List<IEXCloudOHLCVRecord>> response = 
					iexCloudService.fetchOHLCVRange(symbol, currentRange.getRange(), this.apiToken.value())
					.execute();
			
			records = response
					.body()
					.stream()
					.map(record ->  {
						// the endpoint doesn't send a symbol field, hence we need to set it by ourselves
						record.setSymbol(symbol);
						return record;
					})
					.collect(Collectors.toList());
			
			// There are cases where IEXCloud doesn't return records when the
			// range is to large. As a workaround we decrease the range until
			// we get some data back.
			if(records.isEmpty()) {
				LOG.debug("{}: no records received for range {}, response code was: {}:\"{}\"", symbol, currentRange, response.code(), response.message());
				LOG.warn("{}: no records received for range {}, going to decrease range to {}", symbol, currentRange, currentRange.getNextSmallerRange());
				currentRange = currentRange.getNextSmallerRange();
			}
			
		} while(records.isEmpty() && currentRange != null);
		
		if(records.isEmpty() && currentRange == null) {
			throw new NoRecordsProvidedException(symbol, range);
		}
		
		return records;
	}
	
	IEXCloudOHLCVRecord fetchOHLCVPrevious(String symbol) {
		try {
			return iexCloudService.fetchOHLCVPrevious(symbol, this.apiToken.value())
					.execute()
					.body();
		} catch (IOException e) {
			LOG.error(
					String.format("exception during fetchOHLCVPrevious for symbol '%s', returning null", symbol),
					e);
			return null;
		}
	}
	
	
}
