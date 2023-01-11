package io.kipe.streams.kafka.processors.recordtypes;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class TableRecord <K,V>{
	
	@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
	private Map<K,Row<K,V>> rows = new HashMap<>();
	
	public void put(K key, V value) {
		rows.put(key, new Row<>(key, value));
	}
	
	@NoArgsConstructor
	@AllArgsConstructor
	@Getter
	public static class Row<K,V> {
		
		@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
		private K key;
		@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
		private V value;
	}
}
