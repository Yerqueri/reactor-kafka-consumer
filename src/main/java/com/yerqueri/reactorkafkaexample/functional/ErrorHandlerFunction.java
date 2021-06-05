package com.yerqueri.reactorkafkaexample.functional;


import com.yerqueri.reactorkafkaexample.model.EventModel;

/**
 * Created by Gopi K Kancharla
 * 7/23/18 2:22 PM
 */
@FunctionalInterface
public interface ErrorHandlerFunction {

    void errorHandlerCallback(EventModel eventRecord);
}
