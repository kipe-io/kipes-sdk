package de.tradingpulse.connector.iexcloud;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import de.tradingpulse.connector.iexcloud.service.IEXCloudFacade;
import de.tradingpulse.connector.iexcloud.service.IEXCloudOHLCVRecord;

@ExtendWith(MockitoExtension.class)
class IEXCloudOHLCVTaskTest {
	
	@Mock
	private SymbolOffsetProvider symbolOffsetProviderMock;
	
	@Mock
	private IEXCloudFacade iexCloudFacadeMock;
	
	@AfterEach
	void afterEach() {
		verifyNoMoreInteractions(
				symbolOffsetProviderMock,
				iexCloudFacadeMock);
	}

	// ------------------------------------------------------------------------
	// test_internalPoll
	// ------------------------------------------------------------------------
	
	@Test
	void test_internalPoll__on_offset_null__return_null()
	throws InterruptedException
	{
		IEXCloudOHLCVTask task = createTask();
		
		when(symbolOffsetProviderMock.getNextSymbolOffsetForPoll()).thenReturn(null);
		
		assertNull(task.internalPoll());
	}
	
	@Test
	void test_internalPoll__on_offset_call_facade_when_empty_response__return_null()
	throws InterruptedException
	{
		IEXCloudOHLCVTask task = createTask();
		
		when(symbolOffsetProviderMock.getNextSymbolOffsetForPoll()).thenReturn(createSymbolOffset());
		when(iexCloudFacadeMock.fetchOHLCVSince(any(), any())).thenReturn(Collections.emptyList());

		assertNull(task.internalPoll());
		// verification happens at #afterEach
	}
	
	@Test
	void test_internalPoll__on_offset_call_facade_when_response__return_list()
	throws InterruptedException
	{
		IEXCloudOHLCVTask task = createTask();
		
		when(symbolOffsetProviderMock.getNextSymbolOffsetForPoll()).thenReturn(createSymbolOffset());
		when(iexCloudFacadeMock.fetchOHLCVSince(any(), any())).thenReturn(Arrays.asList(createRecord()));
		doNothing().when(symbolOffsetProviderMock).updateOffsets(any());
		
		assertNotNull(task.internalPoll());
		// verification happens at #afterEach
	}
	
	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------
	
	private IEXCloudOHLCVRecord createRecord() {
		IEXCloudOHLCVRecord record = new IEXCloudOHLCVRecord();
		record.setSymbol("symbol");
		record.setDate("2020-10-28");
		
		return record;
	}
	
	private SymbolOffset createSymbolOffset() {
		LocalDate lastFetchedDate = LocalDate.of(2020, 10, 28); // Wednesday
		
		return new SymbolOffset("symbol", lastFetchedDate);
	}
	
	private IEXCloudOHLCVTask createTask() {
		IEXCloudOHLCVTask task = new IEXCloudOHLCVTask();
		task.config = createConfig();
		task.symbolOffsetProvider = symbolOffsetProviderMock;
		task.iexCloudFacade = iexCloudFacadeMock;
		
		return task;
	}
	
	private IEXCloudConnectorConfig createConfig() {
		Map<String, String> props = new HashMap<>();
		props.put(IEXCloudConnectorConfig.CONFIG_KEY_IEX_API_BASEURL, "url");
		props.put(IEXCloudConnectorConfig.CONFIG_KEY_IEX_API_SECRET, "secret");
		props.put(IEXCloudConnectorConfig.CONFIG_KEY_IEX_API_TOKEN, "token");
		props.put(IEXCloudConnectorConfig.CONFIG_KEY_SYMBOLS, "s1, s2, s3");
		props.put(IEXCloudConnectorConfig.CONFIG_KEY_TOPIC, "topic");
		
		return new IEXCloudConnectorConfig(props);
	}
}
