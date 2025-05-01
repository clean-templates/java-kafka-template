package com.rolandsall.producer.async;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class DemoProduceCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println("Executing Demo Callback");
        if (e != null) {
            System.err.println("Error sending message: " + e.getMessage());
        } else {
            System.out.printf("Message sent to partition %d with offset %d%n",
                    recordMetadata.partition(), recordMetadata.offset());
        }

    }
}
