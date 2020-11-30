package de.tradingpulse.connector.iexcloud.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import de.tradingpulse.common.utils.TimeUtils;
import retrofit2.Call;
import retrofit2.Response;

@ExtendWith(MockitoExtension.class)
class IEXCloudFacadeTest {


	private static final String SYMBOL = "symbol";
	private static final Password API_SECRET = new Password("pass");
	private static final Password API_TOKEN = new Password("pass");

	@Mock private IEXCloudService iexCloudServiceMock;
	
	@Mock private Call<IEXCloudOHLCVRecord> previousCallMock;
	@Mock private Call<List<IEXCloudOHLCVRecord>> rangeCallMock;
	
	
	@AfterEach
	void afterEach() {
		verifyNoMoreInteractions(
				iexCloudServiceMock,
				previousCallMock,
				rangeCallMock);
	}
	
	// ------------------------------------------------------------------------
	// test_internalFetchOHLCVSince
	// ------------------------------------------------------------------------

	@Test
	void test_internalFetchOHLCVSince__lastFetchedDate_yesterday_or_later_returns_empty_list() throws IEXCloudException {
		LocalDate today = LocalDate.now();
		
		IEXCloudFacade facade = createFacade();
		assertTrue(facade.internalFetchOHLCVSince(SYMBOL, today, today.minusDays(1)).isEmpty());
		assertTrue(facade.internalFetchOHLCVSince(SYMBOL, today, today).isEmpty());
		assertTrue(facade.internalFetchOHLCVSince(SYMBOL, today, today.plusDays(1)).isEmpty());
	}
	
	@Test
	void test_internalFetchOHLCVSince__lastFetchedDate_Friday_on_following_Sun_Mon_returns_empty_list() throws IEXCloudException {
		LocalDate friday = createDate("2020-10-23");
		assertEquals(DayOfWeek.FRIDAY, friday.getDayOfWeek());
		
		LocalDate sunday = createDate("2020-10-25");
		assertEquals(DayOfWeek.SUNDAY, sunday.getDayOfWeek());
		assertEquals(2, friday.until(sunday, ChronoUnit.DAYS));
		
		IEXCloudFacade facade = createFacade();
		// next calls do not interact with the REST service
		assertTrue(facade.internalFetchOHLCVSince(SYMBOL, friday, sunday).isEmpty());
		assertTrue(facade.internalFetchOHLCVSince(SYMBOL, friday, sunday.plusDays(1)).isEmpty());
		
		// but when calling from Tuesday there will be a real call
		expectRangeCall("2020-10-26");
		facade.internalFetchOHLCVSince(SYMBOL, friday, sunday.plusDays(2));
		// verification via mock interaction
		
		// and when calling from next Sunday there will be also a real call
		expectRangeCall("2020-10-26");
		facade.internalFetchOHLCVSince(SYMBOL, friday, sunday.plusDays(7));
		// verification via mock interaction
	}
	
	@Test
	void test_internalFetchOHLCVSince__lastFetchedDate_Thursday_on_following_Sun_Mon_returns_empty_list() throws IEXCloudException {
		LocalDate friday = createDate("2020-10-23");
		assertEquals(DayOfWeek.FRIDAY, friday.getDayOfWeek());
		
		LocalDate sunday = createDate("2020-10-25");
		assertEquals(DayOfWeek.SUNDAY, sunday.getDayOfWeek());
		assertEquals(2, friday.until(sunday, ChronoUnit.DAYS));
		
		assertTrue(createFacade().internalFetchOHLCVSince(SYMBOL, friday, sunday).isEmpty());
		assertTrue(createFacade().internalFetchOHLCVSince(SYMBOL, friday, sunday.plusDays(1)).isEmpty());
	}
	
	@Test
	void test_internalFetchOHLCVSince__one_workingday_difference_fetches_previous_day() throws IEXCloudException {
		LocalDate thursday = createDate("2020-10-22");
		assertEquals(DayOfWeek.THURSDAY, thursday.getDayOfWeek());
		
		LocalDate saturday = createDate("2020-10-24");
		assertEquals(DayOfWeek.SATURDAY, saturday.getDayOfWeek());
		
		IEXCloudFacade facade = createFacade();
		// when fetching with a days difference previous endpoint is used
		expectPreviousCall();
		facade.internalFetchOHLCVSince(SYMBOL, thursday, saturday);
		// verification via mock interaction
		
		// likewise when it's on Sundays
		expectPreviousCall();
		facade.internalFetchOHLCVSince(SYMBOL, thursday, saturday.plusDays(1));
		// verification via mock interaction
		
		// likewise when it's on Mondays
		expectPreviousCall();
		facade.internalFetchOHLCVSince(SYMBOL, thursday, saturday.plusDays(2));
		// verification via mock interaction
		
		// but when calling from Tuesday there will be a range call
		expectRangeCall("2020-10-24");
		facade.internalFetchOHLCVSince(SYMBOL, thursday, saturday.plusDays(3));
		// verification via mock interaction
		
		// likewise saturday a week later
		expectRangeCall("2020-10-24");
		facade.internalFetchOHLCVSince(SYMBOL, thursday, saturday.plusDays(7));
		// verification via mock interaction
		
	}
	
	@Test
	void test_internalFetchOHLCVSince__throws_NoRecordsProvidedException__when_service_returns_empty_list() {
		LocalDate monday = createDate("2020-10-26");
		assertEquals(DayOfWeek.MONDAY, monday.getDayOfWeek());
		
		LocalDate friday = monday.plusDays(4);
		assertEquals(DayOfWeek.FRIDAY, friday.getDayOfWeek());
		
		IEXCloudFacade facade = createFacade();
		
		expectRangeCallWithNoRecords();
		assertThrows(NoRecordsProvidedException.class, () -> facade.internalFetchOHLCVSince(SYMBOL, monday, friday));
	}
	
	@Test
	void test_internalFetchOHLCVSince__throws_NoRecordsProvidedException__when_service_returns_old_records() {
		LocalDate monday = createDate("2020-10-26");
		assertEquals(DayOfWeek.MONDAY, monday.getDayOfWeek());
		
		LocalDate friday = monday.plusDays(4);
		assertEquals(DayOfWeek.FRIDAY, friday.getDayOfWeek());
		
		IEXCloudFacade facade = createFacade();
		
		expectRangeCall("2020-10-26");
		assertThrows(NoRecordsProvidedException.class, () -> facade.internalFetchOHLCVSince(SYMBOL, monday, friday));
	}

	// ------------------------------------------------------------------------
	// test_removeAlreadyFetchedDates
	// ------------------------------------------------------------------------

	@Test
	void test_removeAlreadyFetchedDates__when_nothing_fetched__return_all() {
		List<IEXCloudOHLCVRecord> records = Arrays.asList(
				null,
				createRecord("2020-01-01"),
				createRecord("2020-01-02"));
		
		List<IEXCloudOHLCVRecord> cleanedRecords = createFacade()
				.removeAlreadyFetchedDates(records, null);
		
		assertEquals(2, cleanedRecords.size());
	}

	@Test
	void test_removeAlreadyFetchedDates() {
		List<IEXCloudOHLCVRecord> records = Arrays.asList(
				null,
				createRecord("2020-01-01"),
				createRecord("2020-01-02"));
		
		List<IEXCloudOHLCVRecord> cleanedRecords = createFacade()
				.removeAlreadyFetchedDates(records, createDate("2020-01-01"));
		
		assertEquals(1, cleanedRecords.size());
		assertEquals("2020-01-02", cleanedRecords.get(0).getDate());
	}

	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------
	
	private void expectRangeCall(String date) {
		expectRangeCall(Arrays.asList(createRecord(date)));
	}
	
	private void expectRangeCallWithNoRecords() {
		expectRangeCall(Collections.emptyList());
	}
	
	private void expectRangeCall(List<IEXCloudOHLCVRecord> returnedRecords) {
		try {
			when(iexCloudServiceMock.fetchOHLCVRange(any(), any(), any())).thenReturn(rangeCallMock);
			when(rangeCallMock.execute()).thenReturn(Response.success(returnedRecords));
		} catch (IOException e) {
			// won't happen
		}
	}
	
	private void expectPreviousCall() {
		try {
			when(iexCloudServiceMock.fetchOHLCVPrevious(any(), any())).thenReturn(previousCallMock);
			when(previousCallMock.execute()).thenReturn(Response.success(null));
		} catch (IOException e) {
			// won't happen
		}
	}

	private IEXCloudOHLCVRecord createRecord(String date) {
		IEXCloudOHLCVRecord record = new IEXCloudOHLCVRecord();
		record.setDate(date);
		return record;
	}

	private LocalDate createDate(String string) {
		return LocalDate.parse(string, TimeUtils.FORMATTER_YYYY_MM_DD);
	}

	private IEXCloudFacade createFacade() {
		return new IEXCloudFacade(API_TOKEN, API_SECRET,5, iexCloudServiceMock);
	}
	
	

}
