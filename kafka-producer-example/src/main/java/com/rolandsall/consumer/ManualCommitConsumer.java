package com.rolandsall.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ManualCommitConsumer {

    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String TOPIC_NAME = "example-topic";
    private static final String GROUP_ID = "manual-commit-consumer-group3";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "manual-consumer-client");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        final Thread mainThread = Thread.currentThread();

        // Add shutdown hook to handle graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Starting shutdown of consumer");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            System.out.println("Consumer started. Waiting for messages...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    System.out.printf("Consumer received %d records%n", records.count());

                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                                record.partition(), record.offset(), record.key(), record.value());
                    }

                    // to commit the offset uncomment below
                     consumer.commitSync();
                    System.out.println("Committed offsets synchronously");
                }
            }
        } catch (WakeupException e) {
            // Ignore, this is expected during shutdown
            System.out.println("Consumer received shutdown signal");
        } catch (Exception e) {
            System.err.println("Unexpected error in consumer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("Consumer closed");
        }
    }
}