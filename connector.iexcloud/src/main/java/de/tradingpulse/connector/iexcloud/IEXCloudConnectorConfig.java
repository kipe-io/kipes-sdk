package de.tradingpulse.connector.iexcloud;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.util.ConnectorUtils;

class IEXCloudConnectorConfig extends AbstractConfig {

	static final String CONFIG_KEY_IEX_API_BASEURL = "iex_api_base_url";
	static final String CONFIG_KEY_IEX_API_TOKEN = "iex_api_token";
	static final String CONFIG_KEY_INITIAL_TIMERANGE_DAYS = "initial_timerange_days";
	static final String CONFIG_KEY_SYMBOLS = "symbols";
	static final String CONFIG_KEY_TOPIC = "topic";
	
	static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(CONFIG_KEY_IEX_API_BASEURL, Type.STRING, Importance.HIGH, "IEXCloud API base url")
			.define(CONFIG_KEY_IEX_API_TOKEN, Type.PASSWORD, Importance.HIGH, "API Token for IEXCloud API")
			.define(CONFIG_KEY_INITIAL_TIMERANGE_DAYS, Type.INT, 5, Importance.LOW, "When fetching a symbol for the first time, the timerange to use in days")
			.define(CONFIG_KEY_SYMBOLS, Type.LIST, Importance.HIGH, "A list of symbols to fetch ohlcv data for")
			.define(CONFIG_KEY_TOPIC, Type.STRING, Importance.HIGH, "The topic to publish data to");
	
	
	IEXCloudConnectorConfig(Map<String, String> props) {
		super(CONFIG_DEF, props);
		validateConfig();
	}

	private void validateConfig() {
		assertValid(CONFIG_KEY_IEX_API_BASEURL, getIexApiBaseUrl());
		assertValid(CONFIG_KEY_IEX_API_TOKEN, getIexApiToken());
		assertValid(CONFIG_KEY_SYMBOLS, getSymbols());
		assertValid(CONFIG_KEY_TOPIC, getTopic());
	}
	
	private <T> void assertValid(String key, T value) {
		boolean throwException = value == null;
		throwException = throwException || ( value instanceof String && ((String)value).isEmpty());
		throwException = throwException || ( value instanceof List && ((List<?>)value).isEmpty());
		throwException = throwException || ( value instanceof Password && ((Password)value).value().isEmpty());
		
		if(throwException) {
			throw new ConfigException(String.format("'%s' must be set for connector IEXCloudOHLCVConnector.", key));
		}
	}

	List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> configs = new ArrayList<>();
		
		ConnectorUtils.groupPartitions(getSymbols(), maxTasks).forEach(someSymbols -> {
			Map<String, String> taskConfig = new HashMap<>();
			taskConfig.put(CONFIG_KEY_IEX_API_BASEURL, getIexApiBaseUrl());
			taskConfig.put(CONFIG_KEY_IEX_API_TOKEN, getIexApiToken().value());
			taskConfig.put(CONFIG_KEY_INITIAL_TIMERANGE_DAYS, getInitialTimerangeInDays().toString());
			taskConfig.put(CONFIG_KEY_SYMBOLS, someSymbols.stream().collect(Collectors.joining(",")));
			taskConfig.put(CONFIG_KEY_TOPIC, getTopic());
			
			configs.add(taskConfig);
		});
		
		return configs;
	}
		
	String getIexApiBaseUrl() {
		return getString(CONFIG_KEY_IEX_API_BASEURL);
	}
	
	Password getIexApiToken() {
		return getPassword(CONFIG_KEY_IEX_API_TOKEN);
	}
	
	Integer getInitialTimerangeInDays() {
		return getInt(CONFIG_KEY_INITIAL_TIMERANGE_DAYS);
	}
	
	List<String> getSymbols() {
		return getList(CONFIG_KEY_SYMBOLS);
	}
	
	String getTopic() {
		return getString(CONFIG_KEY_TOPIC);
	}
	
	public String toString() {
		return String.format(
				"IEXCloudConnectorConfig[%s=%s, %s=%s, %s=%s, %s=%s, %s=%s]", 
				CONFIG_KEY_IEX_API_BASEURL, getIexApiBaseUrl(),
				CONFIG_KEY_IEX_API_TOKEN, getIexApiToken(),
				CONFIG_KEY_INITIAL_TIMERANGE_DAYS, getInitialTimerangeInDays(),
				CONFIG_KEY_SYMBOLS, getSymbols(),
				CONFIG_KEY_TOPIC, getTopic());
	}
}
