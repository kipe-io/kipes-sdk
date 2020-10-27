package de.tradingpulse.common.stream.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OHLCVDataRaw {

    private String symbol;
    private String date;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Long volume;
}
