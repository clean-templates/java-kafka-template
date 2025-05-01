package com.rolandsall.order_example.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rolandsall.order_example.producer.AsynchronousOrderProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class OrderNYStreamProcessor {

    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String SOURCE_TOPIC = "orders-topic";
    private static final String TARGET_TOPIC = "order-ny";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        // Configure the Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-ny-filter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Create the topology
        StreamsBuilder builder = new StreamsBuilder();

        // Read from the source topic
        KStream<String, String> orderStream = builder.stream(
            SOURCE_TOPIC,
            Consumed.with(Serdes.String(), Serdes.String())
        );

       orderStream.filter((key, value) -> {
            try {
                AsynchronousOrderProducer.Order order = objectMapper.readValue(value, AsynchronousOrderProducer.Order.class);
                return order.getAddress() != null && order.getAddress().contains("New York");
            } catch (IOException e) {
                System.err.println("Error parsing order JSON: " + e.getMessage());
                return false;
            }
        }).to(TARGET_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);



        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            System.out.println("State changed from " + oldState + " to " + newState);
            if (newState == KafkaStreams.State.RUNNING) {
                System.out.println("Stream processing started");
            }
        });

        try {
            createTargetTopic();
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            System.err.println("Stream processing interrupted: " + e.getMessage());
        } finally {
            streams.close();
        }
    }

    private static void createTargetTopic() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            int partitions = 3;
            short replicationFactor = 3;

            NewTopic topic = new NewTopic(TARGET_TOPIC, partitions, replicationFactor);

            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(topic));

            try {
                KafkaFuture<Void> future = result.all();
                future.get(); // This blocks until topic creation is complete
                System.out.println("Target topic created successfully: " + TARGET_TOPIC);
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println("Target topic already exists: " + TARGET_TOPIC);
                } else {
                    System.err.println("Error creating target topic: " + e.getMessage());
                }
            }
        }
    }
}