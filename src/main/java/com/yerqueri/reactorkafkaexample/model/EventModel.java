package com.yerqueri.reactorkafkaexample.model;

import lombok.Data;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by Gopi K Kancharla
 * 7/23/18 2:22 PM
 */
@Data
public class EventModel implements Serializable {
    UUID stockId;
    String stockName;
    float price;
}
