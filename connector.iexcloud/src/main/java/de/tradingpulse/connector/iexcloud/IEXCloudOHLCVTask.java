package de.tradingpulse.connector.iexcloud;

import java.time.LocalDate;
import java.time.LocalDateTime;
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
	/** indicates whether messages were already used (i.e. records fetched) during the task's lifetime */
	boolean messagesUsed = true;
	/** indicates whether we already used more messages than purchased */
	boolean overQuota = false;
	/** flag to mark this task stopped, see {@link #stop()} and {@link #start(Map)} */
	volatile boolean stopped = true;
	
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
		this.stopped = false;
		
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
		// #poll() and #stop() will be called on different threads 
		this.stopped = true;
	}
	
	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		
		// TODO: the chain of methods here grows untestable, simplify
		// I think we should establish some sub-components to encapsulate every
		// part of the logic so that they can be test/mocked in isolation

		List<SourceRecord> sourceRecords = this.overQuota? null: internalPoll();
		
		if(sourceRecords == null && !stopped) {
			checkIEXCloudQuota();
			
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
		
		// TODO: a better implementation would consider exchange closing times
		// - the exchange the stock is traded
		// - the closing times of that exchange
		// - the earliest time IEXCloud would allow to fetch the values

		List<SourceRecord> records = null;
		
		LocalDateTime now = LocalDateTime.now();
		LocalDate yesterday = now.toLocalDate().minusDays(1);
		LocalDateTime retryDateTime = now.minusHours(1);
		
		int i = 0;
		List<SymbolOffset> offsets = this.symbolOffsetProvider.getAllSymbolOffsetsSorted();
		Iterator<SymbolOffset> offsetIterator = offsets.iterator();
		while(records == null && offsetIterator.hasNext() && !this.stopped) {
			i++;
			
			SymbolOffset offset = offsetIterator.next();
			
			// We ignore symbols
			// - which are already up-to-date
			// - which had been fetched within our retry interval but delivered no new records
			// The latter could be the case for
			// - holidays
			// - previous endpoint is not yet updated
			if(offset.isLastFetchedDateBefore(yesterday) && offset.isLastFetchAttemptBefore(retryDateTime))	{
				records = internalPoll(offset);
				LOG.info("fetched {} of {}: {}", i, offsets.size(), offset.getSymbol());
				this.symbolOffsetProvider.udpateLastFetchAttempt(offset.getSymbol(), now);
			}
			
		}
		
		return records;
	}
	
	private List<SourceRecord> internalPoll(SymbolOffset symbolOffset) {
		
		List<IEXCloudOHLCVRecord> records;
		try {
			records = this.iexCloudFacade
					.fetchOHLCVSince(
							symbolOffset.getSymbol(), 
							symbolOffset.getLastFetchedDate());
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
