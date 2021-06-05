package com.yerqueri.reactorkafkaexample.publisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;


@Slf4j
@Component
public class DeadLetterPublisher extends DeadLetterSenderUtil<String,String>{

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @Override
    public SenderOptions<String, String> generateSenderOptions() {
        SenderOptions<String,String> senderOptions = SenderOptions.create();
        return senderOptions.producerProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS)
                .producerProperty(ProducerConfig.CLIENT_ID_CONFIG, "deadletter-producer")
                .producerProperty(ProducerConfig.ACKS_CONFIG, "all")
                .withValueSerializer(new StringSerializer())
                .withKeySerializer(new StringSerializer());
    }

    @Override
    public void sendMessage(Flux<SenderRecord<String, String, String>> outboundFlux) {
        try {
            sender.send(outboundFlux)
                    .doOnError(e -> log.error("->", e))
                    .subscribe(r -> log.info("-> {}", r));
        } catch (Exception ex) {
            log.error("Error Sending/Constructing Producer/Data: {}",ex);

        }
    }



}
