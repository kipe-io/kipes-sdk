package de.tradingpulse.streams.kafka.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import de.tradingpulse.streams.kafka.processors.DedupBuilder.DedupTransformer;

@ExtendWith(MockitoExtension.class)
class DedupTransformerTest {

	@Mock
	private KeyValueStore<String,String> stateStoreMock;
	
	@AfterEach
	void afterEach() {
		verifyNoMoreInteractions(
				stateStoreMock);
	}
	
	// ------------------------------------------------------------------------
	// transform - groupBy
	// ------------------------------------------------------------------------

	@Test
	void test_transform__with_groupKeyFunction_emits_first_value() {
		DedupTransformer<String,String, String,String> t = createGroupByDedupTransformer();
		
		String key = "key";
		String value = "value";
		
		when(stateStoreMock.get(key)).thenReturn(null);
		doNothing().when(stateStoreMock).put(key, value);
		
		KeyValue<String,String> kv = t.transform(key, value);
		
		assertNotNull(kv);
		assertEquals(key, kv.key);
		assertEquals(value, kv.value);
	}
	
	@Test
	void test_transform__with_groupKeyFunction_dedups_second_value() {
		DedupTransformer<String,String, String,String> t = createGroupByDedupTransformer();
		
		String key = "key";
		String v1 = "v1";
		
		when(stateStoreMock.get(key)).thenReturn(null);
		doNothing().when(stateStoreMock).put(key, v1);
		
		KeyValue<String,String> kv = t.transform(key, v1);

		String v2 = "v2";
		when(stateStoreMock.get(key)).thenReturn(v1);
		doNothing().when(stateStoreMock).put(key, v2);

		kv = t.transform(key, v2);
		
		assertNull(kv);
	}
	
	@Test
	void test_transform__with_groupKeyFunction_emits_other_key() {
		DedupTransformer<String,String, String,String> t = createGroupByDedupTransformer();
		
		String k1 = "k1";
		String value = "value";
		
		when(stateStoreMock.get(k1)).thenReturn(null);
		doNothing().when(stateStoreMock).put(k1, value);
		
		KeyValue<String,String> kv = t.transform(k1, value);

		String k2 = "k2";
		when(stateStoreMock.get(k2)).thenReturn(null);
		doNothing().when(stateStoreMock).put(k2, value);

		kv = t.transform(k2, value);
		
		assertNotNull(kv);
		assertEquals(k2, kv.key);
		assertEquals(value, kv.value);
	}

	// ------------------------------------------------------------------------
	// transform - groupDedup
	// ------------------------------------------------------------------------

	@Test
	void test_transform__with_groupDedupFunction_emits_first_value() {
		DedupTransformer<String,String, String,String> t = createGroupDedupDedupTransformer();
		
		String key = "key";
		String value = "value";
		
		when(stateStoreMock.get(key)).thenReturn(null);
		doNothing().when(stateStoreMock).put(key, value);
		
		KeyValue<String,String> kv = t.transform(key, value);
		
		assertNotNull(kv);
		assertEquals(key, kv.key);
		assertEquals(value, kv.value);
	}
	
	@Test
	void test_transform__with_groupDedupFunction_dedups_same_value() {
		DedupTransformer<String,String, String,String> t = createGroupDedupDedupTransformer();
		
		String key = "key";
		String v1 = "v1";
		
		when(stateStoreMock.get(key)).thenReturn(null);
		doNothing().when(stateStoreMock).put(key, v1);
		
		KeyValue<String,String> kv = t.transform(key, v1);

		when(stateStoreMock.get(key)).thenReturn(v1);
		doNothing().when(stateStoreMock).put(key, v1);

		kv = t.transform(key, v1);
		
		assertNull(kv);
	}
	
	@Test
	void test_transform__with_groupDedupFunction_emits_other_value() {
		DedupTransformer<String,String, String,String> t = createGroupDedupDedupTransformer();
		
		String key = "key";
		String v1 = "v1";
		
		when(stateStoreMock.get(key)).thenReturn(null);
		doNothing().when(stateStoreMock).put(key, v1);
		
		KeyValue<String,String> kv = t.transform(key, v1);

		String v2 = "v2";
		when(stateStoreMock.get(key)).thenReturn(v1);
		doNothing().when(stateStoreMock).put(key, v2);

		kv = t.transform(key, v2);
		
		assertNotNull(kv);
		assertEquals(key, kv.key);
		assertEquals(v2, kv.value);
	}
	
	@Test
	void test_transform__with_groupDedupFunction_emits_other_key() {
		DedupTransformer<String,String, String,String> t = createGroupDedupDedupTransformer();
		
		String k1 = "k1";
		String value = "value";
		
		when(stateStoreMock.get(k1)).thenReturn(null);
		doNothing().when(stateStoreMock).put(k1, value);
		
		KeyValue<String,String> kv = t.transform(k1, value);

		String k2 = "k2";
		when(stateStoreMock.get(k2)).thenReturn(null);
		doNothing().when(stateStoreMock).put(k2, value);

		kv = t.transform(k2, value);
		
		assertNotNull(kv);
		assertEquals(k2, kv.key);
		assertEquals(value, kv.value);
	}

	// ------------------------------------------------------------------------
	// utils
	// ------------------------------------------------------------------------

	/**
	 * <pre>
	 * groupKeyFunction  : (key, value) -> key
	 * groupDedupFunction: null
	 * </pre>
	 */
	private DedupTransformer<String,String, String,String> createGroupByDedupTransformer() {
		DedupTransformer<String,String, String,String> t = new DedupTransformer<>(
				null, 
				(key, value) -> key, 
				null);
		
		t.stateStore = stateStoreMock;
		return t;
	}

	/**
	 * <pre>
	 * groupKeyFunction  : (key, value) -> key
	 * groupDedupFunction: (key, value) -> value
	 * </pre>
	 */
	private DedupTransformer<String,String, String,String> createGroupDedupDedupTransformer() {
		DedupTransformer<String,String, String,String> t = new DedupTransformer<>(
				null, 
				(key, value) -> key, 
				(key, value) -> value);
		
		t.stateStore = stateStoreMock;
		return t;
	}
}
