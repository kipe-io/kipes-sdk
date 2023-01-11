package de.tradingpulse.stage.backtest.streams;

import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.BacktestStageConstants;
import de.tradingpulse.stage.backtest.recordtypes.BacktestResultRecord;
import io.kipe.streams.kafka.factories.AbstractStreamFactory;
import io.micronaut.configuration.kafka.serde.JsonSerdeRegistry;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
public class BacktestResultStream extends AbstractStreamFactory {

	static final String TOPIC_BACKTESTRESULT_DAILY = BacktestStageConstants.STAGE_NAME + "-" + "backtestresult_daily";

	@Inject
	private JsonSerdeRegistry jsonSerdeRegistry;

	@Override
	protected String[] getTopicNames() {
		return new String[] {
				TOPIC_BACKTESTRESULT_DAILY
		};
	}
	
	@Singleton
    @Named(TOPIC_BACKTESTRESULT_DAILY)
    KStream<SymbolTimestampKey, BacktestResultRecord> backtestResultDailyStream(final ConfiguredStreamBuilder builder) {
		
		return builder
				.stream(TOPIC_BACKTESTRESULT_DAILY, Consumed.with(
						jsonSerdeRegistry.getSerde(SymbolTimestampKey.class), 
						jsonSerdeRegistry.getSerde(BacktestResultRecord.class))
						.withOffsetResetPolicy(AutoOffsetReset.EARLIEST));
    }

}
