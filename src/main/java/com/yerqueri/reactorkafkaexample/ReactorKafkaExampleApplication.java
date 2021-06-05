package com.yerqueri.reactorkafkaexample;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.web.bind.annotation.RestController;


/**
 * Created by Gopi K Kancharla
 * 7/23/18 2:22 PM
 */

@SpringBootApplication
@RestController
@Slf4j
@EnableKafka
@EmbeddedKafka
public class ReactorKafkaExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReactorKafkaExampleApplication.class, args);
    }

//    @RequestMapping(value = "/sendMessage", method = RequestMethod.POST)
//    public ResponseEntity recieveRequest(@RequestBody EventModel incomingMessage) {
//        log.info("Message Recieved: {}", incomingMessage);
//        for(int i=0;i<10000;i++) {
//            Flux<SenderRecord<String, String, String>> outboundFlux = Flux.just(i)
//                    .map(record -> producer.generateSenderRecord("Key" + UUID.randomUUID(), convertPojoToJson(record), "Transaction_" + UUID.randomUUID(), DemoProducer.TOPIC));
//            producer.sendMessage(outboundFlux);
//        }
//        return ResponseEntity.ok("THANKS");
//    }
//
//    private String convertPojoToJson(int i) {
//        return "Message>>>>>[ "+i+" ]";
//    }

}



