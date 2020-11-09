package de.tradingpulse.connector.iexcloud;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IEXCloudOHLCVConnector extends SourceConnector {

	private static final Logger LOG = LoggerFactory.getLogger(IEXCloudOHLCVConnector.class);
	
	private IEXCloudConnectorConfig config;
	
	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		this.config = new IEXCloudConnectorConfig(props);
		
		LOG.info("IEXCloudOHLCVConnector started with config {}", this.config);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return IEXCloudOHLCVTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		return config.taskConfigs(maxTasks);
	}

	@Override
	public void stop() {
		// nothing to do
	}

	@Override
	public ConfigDef config() {
		return IEXCloudConnectorConfig.CONFIG_DEF;
	}

}
