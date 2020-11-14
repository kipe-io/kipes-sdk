package de.tradingpulse.connector.iexcloud.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class IEXCloudMetadata {

	private Long messageLimit;
	private Long messagesUsed;
	
	/**
	 * Returns the ratio of the used messages. Returns null if either of 
	 * {@link #messageLimit} or {@link #messagesUsed} is null.
	 */
	public Double getUsedMessagesRatio() {
		if(this.messageLimit == null || this.messagesUsed == null) {
			return null;
		}
		
		return ((100 * this.messagesUsed) / (100 * this.messageLimit)) / 100.0;
	}
	
	/**
	 * Returns the number of the messages left. Returns null if either of 
	 * {@link #messageLimit} or {@link #messagesUsed} is null.
	 */
	public Long getMessagesLeft() {
		if(this.messageLimit == null || this.messagesUsed == null) {
			return null;
		}
		
		return this.messageLimit - this.messagesUsed;
	}
}
