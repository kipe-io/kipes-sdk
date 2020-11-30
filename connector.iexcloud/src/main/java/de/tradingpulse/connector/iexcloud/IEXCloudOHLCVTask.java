package de.tradingpulse.connector.iexcloud;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tradingpulse.connector.iexcloud.service.FetchException;
import de.tradingpulse.connector.iexcloud.service.IEXCloudException;
import de.tradingpulse.connector.iexcloud.service.IEXCloudFacade;
import de.tradingpulse.connector.iexcloud.service.IEXCloudMetadata;
import de.tradingpulse.connector.iexcloud.service.IEXCloudOHLCVRecord;
import de.tradingpulse.connector.iexcloud.service.NoRecordsProvidedException;

public class IEXCloudOHLCVTask extends SourceTask {
	
	private static final Logger LOG = LoggerFactory.getLogger(IEXCloudOHLCVTask.class);
	
	private static final int CONFIG_POLL_SLEEP_MS = 4000; // stop timeout at Worker.class is 5 secs
	
	IEXCloudConnectorConfig config;
	SymbolOffsetProvider symbolOffsetProvider;
	IEXCloudFacade iexCloudFacade;
	// indicates whether messages were already used (i.e. records fetched) during the task's lifetime
	boolean messagesUsed = true;
	// indicates whether we already used more messages than purchased
	boolean overQuota = false;
	
	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		this.config = new IEXCloudConnectorConfig(props);
		this.symbolOffsetProvider = createSymbolOffsetProvider();
		this.iexCloudFacade = createIEXCloudFacade();

		this.messagesUsed = true;
		this.overQuota = false;

		LOG.info("IEXCloudOHLCVTask started with config {}", this.config);
		
	}

	private SymbolOffsetProvider createSymbolOffsetProvider() {
		return new SymbolOffsetProvider(
				this.config.getSymbols(), 
				this.context.offsetStorageReader());
	}
	
	private IEXCloudFacade createIEXCloudFacade() {
		return new IEXCloudFacade(
				this.config.getIexApiBaseUrl(), 
				this.config.getIexApiToken(),
				this.config.getIexApiSecret(),
				this.config.getInitialTimerangeInDays());
	}
	
	@Override
	public void stop() {
		// Nothing to do
		// #poll() and #stop() will be called on different threads 
	}
	
	@Override
	public List<SourceRecord> poll() throws InterruptedException {

		checkIEXCloudQuota();

		List<SourceRecord> sourceRecords = this.overQuota? null: internalPoll();
		
		if(sourceRecords == null) {
			// let's wait as there is nothing to do right now
			Thread.sleep(CONFIG_POLL_SLEEP_MS);
		}

		return sourceRecords;
	}
	
	void checkIEXCloudQuota() {
		if(!this.messagesUsed && !this.overQuota) {
			return;
		}
		
		IEXCloudMetadata metadata = this.iexCloudFacade.fetchMetadata();
		
		if(metadata == null) {
			LOG.warn("Couldn't fetch quota information.");
			return;
		}
		
		LOG.info("{}/{} ({}%) IEXCloud messages used, {} messages left", 
				metadata.getMessagesUsed(),
				metadata.getMessageLimit(),
				metadata.getUsedMessagesRatio()*100,
				metadata.getMessagesLeft());
		
		this.messagesUsed = false;
		this.overQuota = metadata.getMessagesUsed() >= metadata.getMessageLimit();
	}
	
	List<SourceRecord> internalPoll() {
		
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
		
		List<SourceRecord> records = null;
		LocalDate yesterday = LocalDate.now().minusDays(1);
		
		Iterator<SymbolOffset> offsetIterator = this.symbolOffsetProvider.getAllSymbolOffsetsSorted().iterator();
		while(records == null && offsetIterator.hasNext()) {
			SymbolOffset symbolOffset = offsetIterator.next();
			
			// ignore already up-to-date offsets
			if(	symbolOffset.lastFetchedDate == null
				|| symbolOffset.lastFetchedDate.isBefore(yesterday)) 
			{
				records = internalPoll(symbolOffset);
			}
		}
		
		return records;
	}
	
	List<SourceRecord> internalPoll(SymbolOffset symbolOffset) {
		if(symbolOffset == null) {
			// means nothing to fetch
			
			// returning null following the specification
			return null;
		}

		List<IEXCloudOHLCVRecord> records;
		try {
			records = this.iexCloudFacade
					.fetchOHLCVSince(
							symbolOffset.symbol, 
							symbolOffset.lastFetchedDate);
		} catch (NoRecordsProvidedException e) {
			LOG.error(e.getMessage());
			
			// IEXCloudFacade doesn't return records for stocks which aren't
			// traded anymore. The facade compares the lastFetchedOffset with 
			// the current date and if the lastFetchedOffset was before current
			// decides to throw this exception.
			// It's safe to reomve them from the config.
			// FIXME/TODO: Edge case: closed exchanges for public holidays
			// The logic would potentially remove all affected symbols. 
			// current workaround: restart the service.
			LOG.warn("{}: removing symbol from config. This is not permanent.", e.getSymbol());
			this.symbolOffsetProvider.removeSymbolFromConfig(e.getSymbol());
			
			// returning null following the specification
			return null;
			
		} catch (FetchException e) {
			LOG.error("FetchException while polling for new records: '{}'", e.getMessage());
			// returning null following the specification
			return null;
			
		} catch (IEXCloudException e) {
			throw new RuntimeException(e);
		}
		
		if(records.isEmpty()) {
			// We don't need to fetch already known information. In this case
			// an empty list is returned.
			
			// returning null following the specification
			return null;
		}
		
		this.messagesUsed = true;
		this.symbolOffsetProvider.updateOffsets(records);

		LOG.info("{} record(s) fetched for symbol '{}'", 
				records.size(), 
				records.get(0).getSymbol());
		

		return parseSourceRecords(records);
		
	}
	
	private List<SourceRecord> parseSourceRecords(List<IEXCloudOHLCVRecord> records) {
		return records.stream()
				.map(record -> {
					SymbolOffset so = SymbolOffsetProvider.createSymbolOffset(record);
					return new SourceRecord(
							so.asKafkaConnectPartition(), 
							so.asKafkaConnectOffset(), 
							this.config.getTopic(), 
							null, // partition
							null, // key schema
							null, // key object
							IEXCloudOHLCVRecord.SCHEMA, 
							record.asStruct(),
							record.getLocalDate().atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000);
							
				})
				.collect(Collectors.toList());
	}
}
