package de.tradingpulse.stage.backtest.service.processors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.GenericRecord;
import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.BacktestStageConstants;
import de.tradingpulse.stage.backtest.recordtypes.BacktestResultRecord;
import de.tradingpulse.stage.backtest.streams.BacktestStreamsFacade;
import de.tradingpulse.streams.kafka.factories.AbstractProcessorFactory;
import de.tradingpulse.streams.kafka.processors.TopologyBuilder;
import de.tradingpulse.streams.kafka.processors.expressions.stats.Count;
import de.tradingpulse.streams.kafka.processors.recordtypes.TableRecord;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;

@Singleton
class BacktestResultAnalyticsProcessor extends AbstractProcessorFactory {
	
	private static final String TOPIC_BACKTEST_ANALYTICS = BacktestStageConstants.STAGE_NAME + "-backtest_analytics";
	
	@Inject
	BacktestStreamsFacade backtestStreamsFacade;
	
	@Inject
	ConfiguredStreamBuilder streamBuilder;

	@Inject
	JsonSerdeRegistry jsonSerdeRegistry;

	
	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_BACKTEST_ANALYTICS
		};
	}
	
	@Override
	protected void initProcessors() throws Exception {
		createTopology(
				TOPIC_BACKTEST_ANALYTICS, 
				backtestStreamsFacade.getBacktestResultDailyStream());
	}
	
	@SuppressWarnings("unchecked")
	void createTopology(
			String topic,
			KStream<SymbolTimestampKey, BacktestResultRecord> backtestResults) 
	{
		// --------------------------------------------------------------------
		// from
		//   backtestResultsDaily
		//
		// transform
		//   change GenericRecord.from(value)
		//   as GenericRecord
		//
		// eval
		//   revenueRatio = revenue / entry
		//
		// bin
		//   field revenueRatio
		//   span 0.1
		//   newField revBin
		//
		// stats
		//   count()
		//   by strategyKey, tradingDirection, revBin
		//
		// table
		//
		// to
		//   backtestAnalytics
		// --------------------------------------------------------------------
		//
		// Note to convert the table into TSV
		// ----------------------------------
		// 1) find the last offset
		//
		//    $ kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list 127.0.0.1:9092 --topic stg-backtest-backtest_analytics --time -1 --offsets 1
		//
		// 2) with that partition and offset
		//
		//   $ kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic stg-backtest-backtest_analytics --partition PARTITION --offset OFFSET-1 | jq -rc '.rows | values[] | {revBin: .value.fields.revBin, count: .value.fields.count[1]} | map(.) | @tsv ' -
		
		TopologyBuilder
		.init(streamBuilder)
		.from(
				backtestResults, 
				jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
				jsonSerdeRegistry.getSerde(BacktestResultRecord.class))
		
		.withTopicsBaseName(BacktestStreamsFacade.TOPIC_BACKTESTRESULT_DAILY)
		
		.<SymbolTimestampKey,GenericRecord> transform() 
			.changeValue(
					(key,value) ->
						GenericRecord.create()
						.with("symbol", value.getKey().getSymbol())
						.with("strategyKey", value.getStrategyKey())
						.with("tradingDirection", value.getTradingDirection().name())
						.with("entryTimestamp", value.getEntryTimestamp())
						.with("exitTimestamp", value.getKey().getTimestamp())
						.with("entry", value.getEntry())
						.with("exit", value.getExit())
						.with("revenue", value.getRevenue()))
			.asValueType(
					jsonSerdeRegistry.getSerde(GenericRecord.class))
		
		.withTopicsBaseName(topic)
		
		.eval()
			.with("revenueRatio", (key, value) -> value.getDouble("revenue") / value.getDouble("entry"))
			.build()
		
		.bin()
			.field("revenueRatio")
			.span(0.01)
			.newField("revBin")
			.build()
			
		.stats()
			.with(Count.count())
			.groupBy("revBin")
			.build(jsonSerdeRegistry.getSerde(String.class))
		
		.table()
			.build(
					jsonSerdeRegistry.getSerde(String.class),
					jsonSerdeRegistry.getSerde(
							(Class<TableRecord<String,GenericRecord>>)
							(Class<?>) TableRecord.class))
			
		.to(topic);
		
	}

}
