package com.yerqueri.reactorkafkaexample.publisher;

import com.yerqueri.reactorkafkaexample.ReceiverRecordException;

public interface DeadLetterPublishable {
    public void publish(ReceiverRecordException receiverRecordException);
}
