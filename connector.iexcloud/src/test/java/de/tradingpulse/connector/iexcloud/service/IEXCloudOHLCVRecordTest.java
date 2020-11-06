package de.tradingpulse.connector.iexcloud.service;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

class IEXCloudOHLCVRecordTest {

	@Test
	void test_serde() throws JsonMappingException, JsonProcessingException {
		IEXCloudOHLCVRecord r = new IEXCloudOHLCVRecord();
		
		r.setDate("date");
		r.setSymbol("symbol");
		
		r.setOpen(1.0);
		r.setHigh(2.0);
		r.setLow(3.0);
		r.setClose(4.0);
		r.setVolume(5L);
		
		r.setUOpen(10.0);
		r.setUHigh(20.0);
		r.setULow(30.0);
		r.setUClose(40.0);
		r.setUVolume(50L);
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(r);
		
		IEXCloudOHLCVRecord dr = mapper.readValue(json, IEXCloudOHLCVRecord.class);
		
		assertEquals(r,dr);
	}

}
