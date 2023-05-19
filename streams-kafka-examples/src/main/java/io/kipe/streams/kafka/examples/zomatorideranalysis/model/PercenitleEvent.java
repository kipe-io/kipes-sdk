package io.kipe.streams.kafka.examples.zomatorideranalysis.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PercenitleEvent {
    LocalDate date;
    long riderId;
    double speed;
}
