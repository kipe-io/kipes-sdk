package de.tradingpulse.connector.iexcloud;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tradingpulse.connector.iexcloud.service.IEXCloudOHLCVRecord;

class SymbolOffsetProvider {

	// ------------------------------------------------------------------------
	// static
	// ------------------------------------------------------------------------

	private final static Logger LOG = LoggerFactory.getLogger(SymbolOffsetProvider.class);
	
	/**
	 * Returns all partitions for the given symbols. A partition is essentially 
	 * a one-element-map: 
	 * <pre>
	 * { "symbol": "<symbol>" }
	 * </pre>
	 */
	static Set<Map<String,String>> createKafkaPartitionsFrom(Collection<String> symbols) {
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
	final Set<Map<String, String>> partitions = new HashSet<>();
	// delivers initial offsets
	final OffsetStorageReader offsetStorageReader;
	// stores in-flight offsets
	final Map<String, SymbolOffset> updatedOffsetMap = new HashMap<>();
	// ------------------------------------------------------------------------
	// constructors
	// ------------------------------------------------------------------------

	SymbolOffsetProvider(List<String> symbols, OffsetStorageReader offsetStorageReader) {
		this.symbols = new LinkedList<>(symbols);
		this.offsetStorageReader = offsetStorageReader;
	}
	
	// ------------------------------------------------------------------------
	// methods
	// ------------------------------------------------------------------------

	synchronized void updateOffsets(List<IEXCloudOHLCVRecord> records) {
		if(records == null) {
			return;
		}

		// Note that referencing the latest offset only in memory. This comes
		// with a lot of potential issues. Most notably: When these are stored
		// here the records haven't even been tried to move to Kafka. So it is
		// well possible they won't end up Kafka at all. 
		// However, we need to do this here, as the offsetStorageReader is only
		// a reliable source of truth for the init case.
		//
		// See https://cwiki.apache.org/confluence/display/KAFKA/KIP-618%3A+Atomic+commit+of+source+connector+records+and+offsets
		// for a proper solution. 

		findLatestOffsets(records).forEach(offset -> {
			SymbolOffset oldOffset = this.updatedOffsetMap.get(offset.symbol);
			
			if(	oldOffset != null 
				&& oldOffset.lastFetchedDate != null
				&& !oldOffset.lastFetchedDate.isBefore(offset.lastFetchedDate)) {
				// In case companies get removed from the stock market the old
				// data is still available. However, since it makes no sense to
				// fetch that data further, we remove the symbol.
				
				// FIXME/TODO: Edge case: closed exchanges for public holidays
				// The logic would potentially remove all affected symbols. 
				// current workaround: restart the service.
				LOG.warn(
						"{}: no later offset fetched than already seen. old: {}, new: {}. Going to remove this symbol. This is not permanent, consider to remove it from the config", 
						offset.symbol,
						oldOffset.lastFetchedDate,
						offset.lastFetchedDate);
				
				removeSymbolFromConfig(offset.symbol);
				
			} else {
				
				LOG.debug("{}: updating lastFetchedDate from {} to {}", 
						offset.symbol,
						oldOffset == null? "null" : oldOffset.lastFetchedDate,
						offset.lastFetchedDate);
				
				this.updatedOffsetMap.put(offset.symbol, offset);
			}
			
			
		});
	}
	
	private Collection<SymbolOffset> findLatestOffsets(List<IEXCloudOHLCVRecord> records) {
		Map<String, SymbolOffset> latestOffsetMap = new HashMap<>();
		
		records.forEach(record -> {
			SymbolOffset lastOffset = latestOffsetMap.get(record.getSymbol());
			if(	lastOffset == null 
				|| lastOffset.lastFetchedDate.isBefore(record.getLocalDate())) 
			{
				latestOffsetMap.put(record.getSymbol(), createSymbolOffset(record));
			}
		});
		LOG.debug("latestOffsets: {}", latestOffsetMap.values());
		return latestOffsetMap.values();
	}
	
	synchronized void removeSymbolFromConfig(String symbol) {
		this.symbols.remove(symbol);
		this.partitions.clear();
		this.updatedOffsetMap.clear();
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
		// make sure our internal cache is clean
		this.partitions.clear();
		this.updatedOffsetMap.clear();
		
		this.partitions.addAll(createKafkaPartitionsFrom(symbols));
		
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
