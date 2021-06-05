package com.yerqueri.reactorkafkaexample.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yerqueri.reactorkafkaexample.ReceiverRecordException;
import com.yerqueri.reactorkafkaexample.model.DeadLetterRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderRecord;

import java.util.Date;
import java.util.UUID;

public abstract class DeadLetterSenderUtil<K, V> extends KafkaProducerUtil<K, String, String> implements DeadLetterPublishable {

    @Override
    public void publish(ReceiverRecordException receiverRecordException) {
        DeadLetterRecord<K, V> deadLetterRecord = new DeadLetterRecord<>();
        ReceiverRecord receiverRecord = receiverRecordException.getRecord();
        deadLetterRecord.setValue((V) receiverRecord.value());
        deadLetterRecord.setKey((K) receiverRecord.key());
        deadLetterRecord.setTimeStamp(new Date());
        deadLetterRecord.setMessage(receiverRecordException.getMessage());
        Flux<SenderRecord<K, String, String>> outboundFlux = Flux.just(deadLetterRecord)
                .map(record -> generateSenderRecord(record.getKey(), serializeToString(record), "Transaction_" + UUID.randomUUID(), "deadletter_" + receiverRecord.topic()));
        sendMessage(outboundFlux);
    }

    private String serializeToString(DeadLetterRecord<K, V> deadLetterRecord) {
        ObjectMapper mapper = new ObjectMapper();
        String jsonInString = null;
        try {
            jsonInString = mapper.writeValueAsString(deadLetterRecord);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return jsonInString;
    }

}
