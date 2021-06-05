package com.yerqueri.reactorkafkaexample.publisher;

import com.yerqueri.reactorkafkaexample.ReceiverRecordException;
import com.yerqueri.reactorkafkaexample.model.DeadLetterRecord;
import reactor.kafka.receiver.ReceiverRecord;

public interface DeadLetterPublishable{
    public  void publish(ReceiverRecordException receiverRecordException);
}
