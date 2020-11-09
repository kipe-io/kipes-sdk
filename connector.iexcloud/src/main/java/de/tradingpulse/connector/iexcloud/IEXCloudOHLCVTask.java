package de.tradingpulse.connector.iexcloud;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tradingpulse.connector.iexcloud.service.IEXCloudFacade;
import de.tradingpulse.connector.iexcloud.service.IEXCloudOHLCVRecord;

public class IEXCloudOHLCVTask extends SourceTask {
	
	private static final Logger LOG = LoggerFactory.getLogger(IEXCloudOHLCVTask.class);
	
	IEXCloudConnectorConfig config;
	SymbolOffsetProvider symbolOffsetProvider;
	IEXCloudFacade iexCloudFacade;
	
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
				this.config.getIexApiToken());
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
			LOG.info("no records fetched for symbols {}, going to sleep({}). Consider to prolong '{}' if you see this message too often.", 
					this.config.getSymbols(), 
					this.config.getPollSleepMillis(),
					IEXCloudConnectorConfig.CONFIG_KEY_POLL_SLEEP_MILLIS);

			// let's wait as there is nothing to do right now
			Thread.sleep(this.config.getPollSleepMillis());
		
		} else {
			LOG.info("{} record(s) fetched for symbols {}", 
					sourceRecords.size(), 
					this.config.getSymbols());
			
		}

		return sourceRecords;
	}
	
	List<SourceRecord> internalPoll() {
		SymbolOffset symbolOffset = this.symbolOffsetProvider.getNextSymbolOffsetForPoll();
		
		if(symbolOffset == null) {
			// means nothing to fetch
			
			// returning null following the specification
			return null;
		}

		List<IEXCloudOHLCVRecord> records = this.iexCloudFacade
				.fetchOHLCVSince(
						symbolOffset.symbol, 
						symbolOffset.lastFetchedDate);
		
		if(records.isEmpty()) {
			// returning null following the specification
			return null;
		}
		
		this.symbolOffsetProvider.updateOffsets(records);

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
							IEXCloudOHLCVRecord.SCHEMA, 
							record.asStruct());
							
				})
				.collect(Collectors.toList());
	}
}
