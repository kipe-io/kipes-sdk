package de.tradingpulse.connector.iexcloud.service;

import java.time.LocalDate;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kipe.common.utils.TimeUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class IEXCloudOHLCVRecord {

	public static final Schema SCHEMA = SchemaBuilder.struct()
			.name("IEXCloudOHLCVRecord")
			.field("date", Schema.STRING_SCHEMA)
			.field("symbol", Schema.STRING_SCHEMA)
			.field("open", Schema.OPTIONAL_FLOAT64_SCHEMA)
			.field("high", Schema.OPTIONAL_FLOAT64_SCHEMA)
			.field("low", Schema.OPTIONAL_FLOAT64_SCHEMA)
			.field("close", Schema.OPTIONAL_FLOAT64_SCHEMA)
			.field("volume", Schema.OPTIONAL_INT64_SCHEMA)
			.field("uOpen", Schema.OPTIONAL_FLOAT64_SCHEMA)
			.field("uHigh", Schema.OPTIONAL_FLOAT64_SCHEMA)
			.field("uLow", Schema.OPTIONAL_FLOAT64_SCHEMA)
			.field("uClose", Schema.OPTIONAL_FLOAT64_SCHEMA)
			.field("uVolume", Schema.OPTIONAL_INT64_SCHEMA)
			.build();
			
	private String date;
	private String symbol;
	
	private Double open;
	private Double high;
	private Double low;
	private Double close;
	private Long volume;

	@JsonProperty("uOpen")
	private Double uOpen;
	@JsonProperty("uHigh")
	private Double uHigh;
	@JsonProperty("uLow")
	private Double uLow;
	@JsonProperty("uClose")
	private Double uClose;
	@JsonProperty("uVolume")
	private Long uVolume;
	
	@JsonIgnore
	public LocalDate getLocalDate() {
		return LocalDate.parse(date, TimeUtils.FORMATTER_YYYY_MM_DD);
	}
	
	@JsonIgnore
	public Struct asStruct() {
		Struct struct = new Struct(SCHEMA);
		struct.put("date", this.date);
		struct.put("symbol", this.symbol);
		
		struct.put("open", this.open);
		struct.put("high", this.high);
		struct.put("low", this.low);
		struct.put("close", this.close);
		struct.put("volume", this.volume);
		
		struct.put("uOpen", this.uOpen);
		struct.put("uHigh", this.uHigh);
		struct.put("uLow", this.uLow);
		struct.put("uClose", this.uClose);
		struct.put("uVolume", this.uVolume);
		
		return struct;
	}
}
