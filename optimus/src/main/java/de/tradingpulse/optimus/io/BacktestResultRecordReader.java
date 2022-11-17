package de.tradingpulse.optimus.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.tradingpulse.stage.backtest.recordtypes.BacktestResultRecord;

public class BacktestResultRecordReader {

	public static List<BacktestResultRecord> read(String filePath) {
		return new BacktestResultRecordReader(filePath)
				.readBacktestResultRecords();
	}
	
	private final String filePath;
	
	private BacktestResultRecordReader(String filePath) {
		this.filePath = filePath;
	}
	
	public List<BacktestResultRecord> readBacktestResultRecords() {
		
		List<BacktestResultRecord> records = new LinkedList<>();
		
		try (Stream<String> lines = Files.lines(Paths.get(this.filePath))) {
			
			ObjectMapper mapper = new ObjectMapper();
			
			lines.forEach(line -> {
				try {
					BacktestResultRecord record = mapper.readValue(line, BacktestResultRecord.class);
					records.add(record);				
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
			});
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return records;
	}
}
