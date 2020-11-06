package de.tradingpulse.connector.iexcloud.service;

import java.time.LocalDate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import de.tradingpulse.common.utils.TimeUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class IEXCloudOHLCVRecord {

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
}
