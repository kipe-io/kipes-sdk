package de.tradingpulse.connector.iexcloud;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import de.tradingpulse.connector.iexcloud.service.IEXCloudFacade;
import de.tradingpulse.connector.iexcloud.service.IEXCloudOHLCVRecord;

public class IEXCloudOHLCVTask extends SourceTask {

	IEXCloudConnectorConfig config;
	SymbolOffsetProvider symbolOffsetProvider;
	IEXCloudFacade iexCloudFacade;
	
	SymbolOffset lastOffsetForPoll = null;
	LocalDate lastFetchedDate = null;
	
	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		this.config = new IEXCloudConnectorConfig(props);
		this.symbolOffsetProvider = createSymbolOffsetProvider();
		this.iexCloudFacade = createIEXCloudFacade();
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
		// if the last fetched SymbolOffset was null (had nothing to do) we
		// wait iteratively until the next day based on the lastFetchedDate
		
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
							Schema.STRING_SCHEMA, 
							record);
							
				})
				.collect(Collectors.toList());
	}
}
