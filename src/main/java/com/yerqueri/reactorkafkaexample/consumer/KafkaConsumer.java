package com.yerqueri.reactorkafkaexample.consumer;

import com.yerqueri.reactorkafkaexample.model.EventModel;
import com.yerqueri.reactorkafkaexample.model.TopicConfigMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

/**
 * Created by Gopi K Kancharla
 * 7/23/18 2:22 PM
 */
@Component
@Slf4j
public abstract class KafkaConsumer<K,V> {

    public ReceiverOptions<K,V> receiverOptions;
    private Disposable disposable;

    @Autowired
    TopicConfigMapper topicConfigMapper;

    @PostConstruct
    public void reactorConsumer() {
        Map<String, Object> props = topicConfigMapper.getConvertedConfigurations();
        receiverOptions = ReceiverOptions.create(props);
        disposable = consumeMessages();
        log.info("Heartbeat consumer loaded");
    }

    public abstract ReceiverOptions<K,V> enhanceAndSubscribe();

    private Disposable consumeMessages() {
        ReceiverOptions<K,V> options = enhanceAndSubscribe();
        KafkaReceiver<K,V> receiver = KafkaReceiver.create(options);
        Flux<ReceiverRecord<K, V>> inboundFlux = receiver.receive();
        return process(inboundFlux);
    }

    public abstract Disposable process(Flux<ReceiverRecord<K,V>> inboundFlux);

    @PreDestroy
    public void preDestroy() {
        log.info("Destroying the Consumer");
        disposable.dispose();
        log.info("Consumer Subscribe Flux disposed");
    }

}