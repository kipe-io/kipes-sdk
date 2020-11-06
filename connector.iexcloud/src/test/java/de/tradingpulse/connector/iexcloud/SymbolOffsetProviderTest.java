package de.tradingpulse.connector.iexcloud;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import de.tradingpulse.common.utils.TimeUtils;

@ExtendWith(MockitoExtension.class)
class SymbolOffsetProviderTest {

	@Mock
	private OffsetStorageReader offsetStorageReaderMock;
	
	@AfterEach
	void afterEach() {
		verifyNoMoreInteractions(
				offsetStorageReaderMock);
	}

	// ------------------------------------------------------------------------
	// test_getNextSymbolOffsetForPoll
	// ------------------------------------------------------------------------
	
	@Test
	void test_getNextSymbolOffsetForPoll__no_offsets_returns_first() {
		
		SymbolOffsetProvider provider = createProvider();
		
		when(offsetStorageReaderMock.offsets(provider.partitions))
		.thenReturn(createPartitionToOffsetMap(
				new String[] {}, 
				new String[] {}));
		
		assertEquals("s1", provider.getNextSymbolOffsetForPoll().symbol);
	}
	
	@Test
	void test_getNextSymbolOffsetForPoll__some_offsets_returns_latest() {
		
		SymbolOffsetProvider provider = createProvider();
		
		when(offsetStorageReaderMock.offsets(provider.partitions))
		.thenReturn(createPartitionToOffsetMap(
				new String[] {"s1", "s3" }, 
				new String[] {"2020-01-01", "2020-01-03"}));
		
		assertEquals("s3", provider.getNextSymbolOffsetForPoll().symbol);
	}
	
	@Test
	void test_getNextSymbolOffsetForPoll__yesterday_offsets_get_ignored() {
		// TODO when we are just seconds to the new day we should wait until the next day
		String yesterday = LocalDate.now().minusDays(1).format(TimeUtils.FORMATTER_YYYY_MM_DD);
		String dayBeforeYesterday = LocalDate.now().minusDays(2).format(TimeUtils.FORMATTER_YYYY_MM_DD);
		
		SymbolOffsetProvider provider = createProvider();
		
		when(offsetStorageReaderMock.offsets(provider.partitions))
		.thenReturn(createPartitionToOffsetMap(
				new String[] {"s1", "s2", "s3" }, 
				new String[] {yesterday, dayBeforeYesterday, yesterday}));
		
		assertEquals("s2", provider.getNextSymbolOffsetForPoll().symbol);
	}
	
	@Test
	void test_getNextSymbolOffsetForPoll__all_uptodate_returns_null() {
		// TODO when we are just seconds to the new day we should wait until the next day
		String yesterday = LocalDate.now().minusDays(1).format(TimeUtils.FORMATTER_YYYY_MM_DD);
		
		SymbolOffsetProvider provider = createProvider();
		
		when(offsetStorageReaderMock.offsets(provider.partitions))
		.thenReturn(createPartitionToOffsetMap(
				new String[] {"s1", "s2", "s3" }, 
				new String[] {yesterday, yesterday, yesterday}));
		
		assertNull(provider.getNextSymbolOffsetForPoll());
	}
	
	// ------------------------------------------------------------------------
	// test_getAllSymbolOffsets
	// ------------------------------------------------------------------------
	
	@Test
	void test_getAllSymbolOffsets__no_offsets_returns_all_with_null_offsets() {
		
		SymbolOffsetProvider provider = createProvider();
		
		when(offsetStorageReaderMock.offsets(provider.partitions))
		.thenReturn(createPartitionToOffsetMap(
				new String[] {}, 
				new String[] {}));
		
		List<SymbolOffset> allOffsets = provider.getAllSymbolOffsetsSorted();
		
		// then there must be 3 offsets
		assertEquals(3, allOffsets.size());
		
		// and all offsets must have null as lastFetchedDate
		allOffsets.forEach((offset) -> assertNull(offset.lastFetchedDate));
		
		// and all symbols must be included
		assertTrue(allOffsets.stream()
		.map((offset) -> {return offset.symbol; })
		.collect(Collectors.toSet())
		.containsAll(provider.symbols));
	}
	
	@Test
	void test_getAllSymbolOffsets__some_offsets_returns_some_with_non_null_offsets() {
		
		SymbolOffsetProvider provider = createProvider();
		
		when(offsetStorageReaderMock.offsets(provider.partitions))
		.thenReturn(createPartitionToOffsetMap(
				new String[] {"s1"}, 
				new String[] {"2020-01-01"}));
		
		List<SymbolOffset> allOffsets = provider.getAllSymbolOffsetsSorted();
		
		// then there must be 3 offsets
		assertEquals(3, allOffsets.size());
		
		// and all offsets must have null as lastFetchedDate
		allOffsets.forEach((offset) -> {
			if(offset.symbol.equals("s1")) {
				assertNotNull(offset.lastFetchedDate);
			} else {
				assertNull(offset.lastFetchedDate);
			}
		});
		
		// and all symbols must be included
		assertTrue(allOffsets.stream()
		.map((offset) -> {return offset.symbol; })
		.collect(Collectors.toSet())
		.containsAll(provider.symbols));
	}

	@Test
	void test_getAllSymbolOffsets__all_offsets_returns_all_with_non_null_offsets() {
		
		SymbolOffsetProvider provider = createProvider();
		
		when(offsetStorageReaderMock.offsets(provider.partitions))
		.thenReturn(createPartitionToOffsetMap(
				new String[] {"s1", "s2", "s3"}, 
				new String[] {"2020-01-01", "2020-01-02", "2020-01-03"}));
		
		List<SymbolOffset> allOffsets = provider.getAllSymbolOffsetsSorted();
		
		// then there must be 3 offsets
		assertEquals(3, allOffsets.size());
		
		// and all offsets must have null as lastFetchedDate
		allOffsets.forEach((offset) -> assertNotNull(offset.lastFetchedDate));
		
		// and all symbols must be included
		assertTrue(allOffsets.stream()
		.map((offset) -> {return offset.symbol; })
		.collect(Collectors.toSet())
		.containsAll(provider.symbols));
	}

	@Test
	void test_getAllSymbolOffsets__returns_sorted() {
		
		SymbolOffsetProvider provider = createProvider();
		
		when(offsetStorageReaderMock.offsets(provider.partitions))
		.thenReturn(createPartitionToOffsetMap(
				new String[] {"s1", "s3"}, 
				new String[] {"2020-01-01", "2020-01-03"}));
		
		List<SymbolOffset> allOffsets = provider.getAllSymbolOffsetsSorted();
		
		assertEquals("s3", allOffsets.get(0).symbol);
		assertEquals("s1", allOffsets.get(1).symbol);
		assertEquals("s2", allOffsets.get(2).symbol);
		
	}

	
	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------
	
	private SymbolOffsetProvider createProvider() {
		
		IEXCloudConnectorConfig config = createConfig();
		
		return new SymbolOffsetProvider(
				config.getSymbols(), 
				offsetStorageReaderMock);
	}
	
	private IEXCloudConnectorConfig createConfig() {
		Map<String, String> props = new HashMap<>();
		props.put(IEXCloudConnectorConfig.CONFIG_KEY_IEX_API_BASEURL, "url");
		props.put(IEXCloudConnectorConfig.CONFIG_KEY_IEX_API_TOKEN, "token");
		props.put(IEXCloudConnectorConfig.CONFIG_KEY_SYMBOLS, "s1, s2, s3");
		props.put(IEXCloudConnectorConfig.CONFIG_KEY_TOPIC, "topic");
		
		return new IEXCloudConnectorConfig(props);
	}
	
	private Map<Map<String, String>, Map<String, Object>> createPartitionToOffsetMap(String[] symbols, String[] offsets) {
		Map<Map<String, String>, Map<String, Object>> p2oMap = new HashMap<>();
		
		for(int i = 0; i < symbols.length; i++) {
			p2oMap.put(partition(symbols[i]), offset(offsets[i]));			
		}
		
		return p2oMap;
	}
	
	private Map<String, String> partition(String symbol) {
		return SymbolOffset.createKafkaPartition(symbol);
	}
	
	private Map<String, Object> offset(String day) {
		return SymbolOffset.createKafkaOffset(LocalDate.parse(day, TimeUtils.FORMATTER_YYYY_MM_DD));
	}
}
