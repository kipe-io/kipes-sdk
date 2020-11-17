package de.tradingpulse.connector.iexcloud.service;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class IEXCloudMetadataTest {

	@Test
	void test_getUsedMessagesRatio() {
		IEXCloudMetadata d = new IEXCloudMetadata(76L, 43L);
		
		assertEquals(0.566, d.getUsedMessagesRatio());
	}

}
