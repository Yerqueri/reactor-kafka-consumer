package com.yerqueri.reactorkafkaexample.model;

import lombok.Data;

import java.util.Date;

@Data
public class DeadLetterRecord<K, V> {
    String message;
    Date timeStamp;
    K key;
    V value;
}
