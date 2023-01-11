package de.tradingpulse.connector.iexcloud;

import static de.tradingpulse.connector.iexcloud.SymbolOffsetProvider.createKafkaPartitionsFrom;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.util.Arrays;
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

import io.kipe.common.utils.TimeUtils;

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
		allOffsets.forEach((offset) -> assertNull(offset.getLastFetchedDate()));
		
		// and all symbols must be included
		assertTrue(allOffsets.stream()
		.map((offset) -> {return offset.getSymbol(); })
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
			if(offset.getSymbol().equals("s1")) {
				assertNotNull(offset.getLastFetchedDate());
			} else {
				assertNull(offset.getLastFetchedDate());
			}
		});
		
		// and all symbols must be included
		assertTrue(allOffsets.stream()
		.map((offset) -> {return offset.getSymbol(); })
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
		allOffsets.forEach((offset) -> assertNotNull(offset.getLastFetchedDate()));
		
		// and all symbols must be included
		assertTrue(allOffsets.stream()
		.map((offset) -> {return offset.getSymbol(); })
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
		
		assertEquals("s3", allOffsets.get(0).getSymbol());
		assertEquals("s1", allOffsets.get(1).getSymbol());
		assertEquals("s2", allOffsets.get(2).getSymbol());
		
	}
	
	@Test
	void test_getAllSymbolOffsets__returns_all_without_the_removed_symbol() {
		
		SymbolOffsetProvider provider = createProvider();
		
		when(offsetStorageReaderMock.offsets(provider.partitions))
		.thenReturn(createPartitionToOffsetMap(
				new String[] {"s1", "s3"}, 
				new String[] {"2020-01-01", "2020-01-03"}));
		
		provider.removeSymbolFromConfig("s2");
		List<SymbolOffset> allOffsets = provider.getAllSymbolOffsetsSorted();
		
		assertEquals(2, allOffsets.size());
		assertEquals("s3", allOffsets.get(0).getSymbol());
		assertEquals("s1", allOffsets.get(1).getSymbol());
		
	}
	
	// ------------------------------------------------------------------------
	// test_removeSymbolFromConfig
	// ------------------------------------------------------------------------
	
	@Test
	void test_removeSymbolFromConfig_removes_that_symbol() {
		// given
		SymbolOffsetProvider provider = createProvider();
		
		when(offsetStorageReaderMock.offsets(provider.partitions))
		.thenReturn(createPartitionToOffsetMap(
				new String[] {}, 
				new String[] {}));
		
		provider.initializeOffsets();

		// when
		provider.removeSymbolFromConfig("s2");

		// then
		assertEquals(2, provider.symbols.size());
		assertFalse(provider.symbols.contains("s2"));
		
		assertEquals(2, provider.updatedOffsetMap.size());
		assertFalse(provider.updatedOffsetMap.containsKey("s2"));
		
		assertEquals(2, provider.partitions.size());
		assertTrue(provider.partitions.stream().filter(map -> map.containsValue("s2")).collect(Collectors.toList()).isEmpty());
	}
	
	// ------------------------------------------------------------------------
	// test_initializeOffsets
	// ------------------------------------------------------------------------
		
	@Test
	void test_initializeOffsets__osrMock_is_called_with_the_correct_partitions() {
		
		SymbolOffsetProvider provider = createProvider();

		provider.removeSymbolFromConfig("s2");
		
		when(offsetStorageReaderMock.offsets(eq(createKafkaPartitionsFrom(Arrays.asList("s1", "s3")))))
		.thenReturn(createPartitionToOffsetMap(
				new String[] {"s1", "s3"}, 
				new String[] {"2020-01-01", "2020-01-03"}));
		
		provider.initializeOffsets();
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
		props.put(IEXCloudConnectorConfig.CONFIG_KEY_IEX_API_SECRET, "secret");
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
