package de.tradingpulse.connector.iexcloud;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.storage.OffsetStorageReader;

import de.tradingpulse.connector.iexcloud.service.IEXCloudOHLCVRecord;

class SymbolOffsetProvider {

	// ------------------------------------------------------------------------
	// static
	// ------------------------------------------------------------------------

	/**
	 * Returns all partitions for the given symbols. A partition is essentially 
	 * a one-element-map: 
	 * <pre>
	 * { "symbol": "<symbol>" }
	 * </pre>
	 */
	static Collection<Map<String,String>> createKafkaPartitionsFrom(Collection<String> symbols) {
		return symbols.stream()
				.map(SymbolOffset::createKafkaPartition)
				.collect(Collectors.toSet());
	}

	static SymbolOffset createSymbolOffset(IEXCloudOHLCVRecord record) {
		return new SymbolOffset(record.getSymbol(), record.getLocalDate());
	}
	
	// ------------------------------------------------------------------------
	// instance
	// ------------------------------------------------------------------------

	final List<String> symbols;
	final Collection<Map<String, String>> partitions;
	final OffsetStorageReader offsetStorageReader;

	// ------------------------------------------------------------------------
	// constructors
	// ------------------------------------------------------------------------

	SymbolOffsetProvider(List<String> symbols, OffsetStorageReader offsetStorageReader) {
		this.symbols = symbols;
		this.partitions = createKafkaPartitionsFrom(symbols);
		this.offsetStorageReader = offsetStorageReader;
	}
	
	// ------------------------------------------------------------------------
	// methods
	// ------------------------------------------------------------------------

	SymbolOffset getNextSymbolOffsetForPoll() {
		
		// I assume #poll() will be called as soon as a worker is ready to 
		// execute this. Furthermore I assume, this will happen quite often.
		//
		// Therefore, the poll logic is:
		// - each poll() shall handle only one symbol
		// - handle the symbol with the least days to fetch first
		// - once nothing needs to get fetched, return null
		
		// TODO: a better implementation would consider exchange closing times
		// - the exchange the stock is traded
		// - the closing times of that exchange
		// - the earliest time IEXCloud would allow to fetch the values
		
		LocalDate yesterday = LocalDate.now().minusDays(1);
		
		return getAllSymbolOffsetsSorted().stream()
		.filter(symbolOffset -> SymbolOffset.compare(yesterday, symbolOffset.lastFetchedDate) == -1)
		.findFirst()
		.orElse(null);
	}
	
	/**
	 * Returns a set of SymbolOffsets for all configured symbols. The returned
	 * collection is sorted so that the first element has the latest
	 * lastFetchedDate.
	 */
	List<SymbolOffset> getAllSymbolOffsetsSorted() {

		Map<String, SymbolOffset> offsetMap = new HashMap<>();
		
		// add the existing offsets
		// partition map: { "symbol": "<symbol>" }
		// offset map: { "lastFetchedDate": "<date as yyyy-MM-dd>" }
		Map<Map<String, String>, Map<String, Object>> partitionToOffsetsMap = 
				offsetStorageReader.offsets(this.partitions);
		
		partitionToOffsetsMap.keySet().forEach(partition -> {
			SymbolOffset so = SymbolOffset.fromKafkaPartitionAndOffset(partition, partitionToOffsetsMap.get(partition));
			offsetMap.put(so.symbol, so);
		});
		
		// add missing offsets
		this.symbols.stream()
		.filter(symbol -> !offsetMap.containsKey(symbol))
		.forEach(symbol -> offsetMap.put(symbol, new SymbolOffset(symbol, null)));
		
		// sort
		List<SymbolOffset> allOffsets = new LinkedList<>(offsetMap.values());
		Collections.sort(allOffsets);
		
		return allOffsets;
	}
}
