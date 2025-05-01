package com.rolandsall.fraud.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TransactionProducerCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            System.err.println("Error sending transaction: " + exception.getMessage());
            exception.printStackTrace();
        } else {
            System.out.println("Transaction sent successfully to topic: " + metadata.topic() +
                    ", partition: " + metadata.partition() +
                    ", offset: " + metadata.offset() +
                    ", timestamp: " + metadata.timestamp());
        }
    }
}