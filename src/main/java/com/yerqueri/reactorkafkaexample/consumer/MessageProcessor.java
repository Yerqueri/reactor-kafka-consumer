package com.yerqueri.reactorkafkaexample.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yerqueri.reactorkafkaexample.ReceiverRecordException;
import com.yerqueri.reactorkafkaexample.model.EventModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;

import javax.annotation.PostConstruct;
import java.util.Random;

/**
 * Created by Gopi K Kancharla
 * 7/24/18 2:32 PM
 */
@Component
@Slf4j
public class MessageProcessor {

    Random r = new Random();

    @PostConstruct
    public void reactorConsumer() {
        log.info("Message processor loaded");
    }

    public ReceiverOffset processMessage(ReceiverRecord<String, String> record) {
        ReceiverOffset offset = record.receiverOffset();
        log.info("record to process -->{}", record);
        try {
            log.info("message->{}", record.value());
            int rand = r.nextInt(100);
            if (rand > 98) {
                throw new ReceiverRecordException(record, new RuntimeException("Random exception"));
            } else {
//                int numCore = 8;
//                int numThreadsPerCore = 2;
//                double load = 0.8;
//                final long duration = 60000;
//                for (int thread = 0; thread < numCore * numThreadsPerCore; thread++) {
//                    new Load.BusyThread("Thread" + thread, load, duration).start();
//                }
            }
        } catch (Exception e) {
            log.error("Error Processing HeartBeat:");
            throw new ReceiverRecordException(record, new RuntimeException("error handling record"));
        }
        return offset;
    }

    private EventModel convertJsonToPOJO(String value) {
        ObjectMapper mapper = new ObjectMapper();
        EventModel em = null;
        try {
            em = mapper.readValue(value, EventModel.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return em;
    }
}
