package com.yerqueri.reactorkafkaexample.consumer;

import com.yerqueri.reactorkafkaexample.ReceiverRecordException;
import com.yerqueri.reactorkafkaexample.publisher.DeadLetterPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Component
@Slf4j
public class StockPriceConsumer extends KafkaConsumer<String, String> {

    public static final String TOPIC = "demo-topic";
    @Autowired
    MessageProcessor messageProcessor;
    @Autowired
    DeadLetterPublisher deadLetterPublisher;
    Map<Integer, List<String>> map = new ConcurrentHashMap<>();

    @Override
    public ReceiverOptions<String, String> enhanceAndSubscribe() {
        return receiverOptions.subscription(Collections.singleton(TOPIC))
                .addAssignListener(partitions -> log.info("Partitions Assigned {}", partitions))
                .addRevokeListener(partitions -> log.info("Partitions Revoked {}", partitions))
                .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "CUSTOM_GROUP")
                .commitInterval(Duration.ofSeconds(10))
                .commitBatchSize(5);
    }

    @Override
    public Disposable process(Flux<ReceiverRecord<String, String>> inboundFlux) {
        Scheduler scheduler = Schedulers.newParallel("FLUX_DEFER", 100);

        return inboundFlux
                .groupBy(record -> record.receiverOffset().topicPartition())
                .flatMap(partitionFlux -> partitionFlux
                        .publishOn(scheduler)
                        .concatMap(el -> Flux.just(el)
                                .publishOn(scheduler)
                                .doOnNext(receiverRecord -> {
                                    log.info("Starting to process {}", receiverRecord);
                                    messageProcessor.processMessage(receiverRecord);
                                    receiverRecord.receiverOffset().acknowledge();
                                    List<String> partitionList = map.getOrDefault(receiverRecord.partition(), new ArrayList<>());
                                    partitionList.add((String) receiverRecord.value());
                                    map.put(receiverRecord.partition(), partitionList);
                                    log.info("Message acknowledged");
                                })
                                .doOnError(e -> log.error("ERRRRRRROOORRRRRR"))
                                .retryWhen(Retry.backoff(3, Duration.ofSeconds(5)).maxBackoff(Duration.ofSeconds(20)).transientErrors(true))
                                .onErrorResume(e -> {
                                    ReceiverRecordException ex = (ReceiverRecordException) e.getCause();
                                    log.error("Retries exhausted for {}", ex.getRecord().value());
                                    deadLetterPublisher.publish(ex);
                                    log.warn("{} published to dead letter", ex.getRecord().value());
                                    ex.getRecord().receiverOffset().acknowledge();
                                    List<String> partitionList = map.getOrDefault(ex.getRecord().partition(), new ArrayList<>());
                                    partitionList.add((String) ex.getRecord().value());
                                    map.put(ex.getRecord().partition(), partitionList);
                                    return Mono.empty();
                                })
                        ).repeat()
                )
                .subscribeOn(scheduler)
                .subscribe(success -> {
                    log.info("Partition progress is ==>{}", map);
                });

    }

}


