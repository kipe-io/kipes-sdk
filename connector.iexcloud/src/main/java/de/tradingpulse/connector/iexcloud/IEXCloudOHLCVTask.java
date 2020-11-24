package de.tradingpulse.connector.iexcloud;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	boolean messagesUsed = true;
	
	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		this.config = new IEXCloudConnectorConfig(props);
		this.symbolOffsetProvider = createSymbolOffsetProvider();
		this.iexCloudFacade = createIEXCloudFacade();
		
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
		
		List<SourceRecord> sourceRecords = internalPoll();
		if(sourceRecords == null) {
			// let's wait as there is nothing to do right now
			// but before lets check how many messages we do have left
			checkIEXCloudQuota();
			Thread.sleep(CONFIG_POLL_SLEEP_MS);
		}

		return sourceRecords;
	}
	
	void checkIEXCloudQuota() {
		if(!this.messagesUsed) {
			return;
		}
		
		IEXCloudMetadata metadata = this.iexCloudFacade.fetchMetadata();
		
		if(metadata == null) {
			return;
		}
		
		LOG.info("{}/{} ({}%) IEXCloud messages used, {} messages left", 
				metadata.getMessagesUsed(),
				metadata.getMessageLimit(),
				metadata.getUsedMessagesRatio()*100,
				metadata.getMessagesLeft());
		
		this.messagesUsed = false;
	}
	
	List<SourceRecord> internalPoll() {
		SymbolOffset symbolOffset = this.symbolOffsetProvider.getNextSymbolOffsetForPoll();
		
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
			LOG.warn("{}: removing symbol from config. This is not permanent.", e.getSymbol());
			this.symbolOffsetProvider.removeSymbolFromConfig(e.getSymbol());
			
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
