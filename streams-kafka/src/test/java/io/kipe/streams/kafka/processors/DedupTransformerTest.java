/*
 * Kipe Streams Kafka - Kipe Streams SDK
 * Copyright © 2023 Kipe.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.kipe.streams.kafka.processors;

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

import io.kipe.streams.kafka.processors.DedupBuilder.DedupTransformer;


/**
 * Test class for {@link DedupTransformer}.
 */
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

	/**
	 * Test method for {@link DedupTransformer#transform(Object, Object)} with a group by function.
	 * <p>
	 * This test method verifies that the first value with a given key is emitted, but subsequent values with the same
	 * key are not.
	 */
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

	/**
	 * Test method for {@link DedupTransformer#transform(Object, Object)} with a group by function.
	 * <p>
	 * This test method verifies that subsequent values with the same key are not emitted.
	 */
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

	/**
	 * Test method for {@link DedupTransformer#transform(Object, Object)} with a group by function.
	 * <p>
	 * This test method verifies that values with different keys are emitted.
	 */
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

	/**
	 * Test the transform method with a groupDedup function. Expects the first value to be emitted.
	 */
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

	/**
	 * Test the transform method with a groupDedup function. Expects the same value to be deduplicated and not emitted.
	 */
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

	/**
	 * Test for the transform method with a group dedup function.
	 * <p>
	 * Asserts that the method returns a KeyValue with the correct key and value when the state store has a different
	 * value for the key.
	 */
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

	/**
	 * Test for the transform method with a group dedup function.
	 * <p>
	 * Asserts that the method returns a KeyValue with the correct key and value when the input key is different from
	 * the key in the state store.
	 */
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
	 * Creates and returns an instance of the  {@link DedupTransformer} class, with the following configuration:
	 * <pre>
	 * groupKeyFunction : (key, value) -> key
	 * groupDedupFunction: null
	 * </pre>
	 * The stateStore property of the  {@link DedupTransformer} instance is set to the stateStoreMock object.
	 *
	 * @return an instance of the  {@link DedupTransformer} class with the above configuration.
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
	 * Creates and returns an instance of the {@link DedupTransformer} class, with the following configuration:
	 * <pre>
	 * groupKeyFunction : (key, value) -> key
	 * groupDedupFunction: (key, value) -> value
	 * </pre>
	 * The stateStore property of the DedupTransformer instance is set to the stateStoreMock object.
	 *
	 * @return an instance of the  {@link DedupTransformer} class with the above configuration.
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
