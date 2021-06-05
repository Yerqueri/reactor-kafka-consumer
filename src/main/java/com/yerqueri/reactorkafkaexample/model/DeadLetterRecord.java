package com.yerqueri.reactorkafkaexample.model;

import com.yerqueri.reactorkafkaexample.ReceiverRecordException;
import lombok.Data;

import java.util.Date;

@Data
public class DeadLetterRecord<K,V> {
    String message;
    Date timeStamp;
    K key;
    V value;
}
