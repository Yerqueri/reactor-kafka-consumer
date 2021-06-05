package com.yerqueri.reactorkafkaexample.publisher;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;


@Slf4j
public abstract class KafkaProducerUtil<K, V, T> {
    public KafkaSender<K, V> sender;
    public SenderOptions<K, V> senderOptions;

    public void close() {
        sender.close();
    }

    @PreDestroy
    public void destory() {
        close();
    }


    @PostConstruct
    public void initateKafkaProducerConfig() {
        senderOptions = generateSenderOptions();
        sender = KafkaSender.create(senderOptions);
    }

    public abstract SenderOptions<K, V> generateSenderOptions();

    public abstract void sendMessage(Flux<SenderRecord<K, V, T>> outboundFlux);

    @NotNull
    public SenderRecord<K, V, T> generateSenderRecord(K key, V value, T correlationData, String topic) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, key, value);
        return SenderRecord.create(producerRecord, correlationData);
    }

}

