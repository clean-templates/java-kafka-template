package com.rolandsall.order_example.producer;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class OrderProducerCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            System.err.println("Error sending order: " + exception.getMessage());
            exception.printStackTrace();
        } else {
            System.out.println("Order delivered successfully to:");
            System.out.println("  Topic: " + metadata.topic());
            System.out.println("  Partition: " + metadata.partition());
            System.out.println("  Offset: " + metadata.offset());
            System.out.println("  Timestamp: " + metadata.timestamp());
        }
    }
}