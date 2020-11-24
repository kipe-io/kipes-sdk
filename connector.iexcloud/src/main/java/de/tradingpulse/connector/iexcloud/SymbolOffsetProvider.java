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
	// delivers initial offsets
	final OffsetStorageReader offsetStorageReader;
	// stores in-flight offsets
	final Map<String, SymbolOffset> updatedOffsetMap = new HashMap<>();
	// ------------------------------------------------------------------------
	// constructors
	// ------------------------------------------------------------------------

	SymbolOffsetProvider(List<String> symbols, OffsetStorageReader offsetStorageReader) {
		this.symbols = new LinkedList<>(symbols);
		this.partitions = createKafkaPartitionsFrom(symbols);
		this.offsetStorageReader = offsetStorageReader;
	}
	
	// ------------------------------------------------------------------------
	// methods
	// ------------------------------------------------------------------------

	synchronized void updateOffsets(List<IEXCloudOHLCVRecord> records) {
		if(records == null) {
			return;
		}

		// Note that referencing the latest offset like this comes with a lot
		// of potential issues. Most notably: When these are stored here the
		// records haven't even been tried to move to Kafka. So it is well
		// possible they won't end up there. 
		// However, we need to do this here, as the offsetStorageReader is only
		// a reliable source of truth for the init case.
		//
		// See https://cwiki.apache.org/confluence/display/KAFKA/KIP-618%3A+Atomic+commit+of+source+connector+records+and+offsets
		// for a proper solution. 
		
		records.forEach(record -> {
			SymbolOffset lastOffset = this.updatedOffsetMap.get(record.getSymbol());
			if(lastOffset == null 
					|| lastOffset.lastFetchedDate == null
					|| lastOffset.lastFetchedDate.isBefore(record.getLocalDate())) 
			{
				this.updatedOffsetMap.put(record.getSymbol(), createSymbolOffset(record));
			}
		});
	}
	
	synchronized void removeSymbolFromConfig(String symbol) {
		this.symbols.remove(symbol);
		this.updatedOffsetMap.clear();
	}
	
	synchronized SymbolOffset getNextSymbolOffsetForPoll() {
		
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
	synchronized List<SymbolOffset> getAllSymbolOffsetsSorted() {

		if(this.updatedOffsetMap.isEmpty()) {
			initializeOffsets();
		}
				
		// sort
		List<SymbolOffset> allOffsets = new LinkedList<>(this.updatedOffsetMap.values());
		Collections.sort(allOffsets);
		
		return allOffsets;
	}
	
	void initializeOffsets() {
		// add the existing offsets
		// partition map: { "symbol": "<symbol>" }
		// offset map: { "lastFetchedDate": "<date as yyyy-MM-dd>" }
		Map<Map<String, String>, Map<String, Object>> partitionToOffsetsMap = 
				offsetStorageReader.offsets(this.partitions);
		
		partitionToOffsetsMap.keySet().forEach(partition -> {
			SymbolOffset so = SymbolOffset.fromKafkaPartitionAndOffset(partition, partitionToOffsetsMap.get(partition));
			this.updatedOffsetMap.put(so.symbol, so);
		});
		
		// add missing offsets
		this.symbols.stream()
		.filter(symbol -> !this.updatedOffsetMap.containsKey(symbol))
		.forEach(symbol -> this.updatedOffsetMap.put(symbol, new SymbolOffset(symbol, null)));
	}
	
}
