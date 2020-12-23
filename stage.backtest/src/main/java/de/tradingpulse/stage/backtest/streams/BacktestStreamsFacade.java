package de.tradingpulse.stage.backtest.streams;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.kafka.streams.kstream.KStream;

import de.tradingpulse.common.stream.recordtypes.SymbolTimestampKey;
import de.tradingpulse.stage.backtest.recordtypes.BacktestResultRecord;
import de.tradingpulse.stage.backtest.recordtypes.SignalExecutionRecord;
import lombok.Getter;

@Singleton
@Getter
public final class BacktestStreamsFacade {

	@Inject @Named(SignalExecutionStream.TOPIC_SIGNAL_EXECUTION_DAILY)
    private KStream<SymbolTimestampKey, SignalExecutionRecord> signalExecutionDailyStream;
	private final String signalExecutionDailyStreamName = SignalExecutionStream.TOPIC_SIGNAL_EXECUTION_DAILY;

	@Inject @Named(BacktestResultStream.TOPIC_BACKTESTRESULT_DAILY)
    private KStream<SymbolTimestampKey, BacktestResultRecord> backtestResultDailyStream;
	private final String backtestResultDailyStreamName = BacktestResultStream.TOPIC_BACKTESTRESULT_DAILY;

}
