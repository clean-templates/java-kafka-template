package com.rolandsall.fraud.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rolandsall.fraud.model.Transaction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class FraudDetectionStreamProcessor {

    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String SOURCE_TOPIC = "transactions";
    private static final String ALERTS_TOPIC = "fraud-alerts";
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    // Thresholds for fraud detection
    private static final double HIGH_VALUE_THRESHOLD = 1000.0; // Transactions above $1000 are considered high value
    public static final int TRANSACTION_COUNT_THRESHOLD = 3; // More than 3 high-value transactions in window is suspicious
    public static final double AMOUNT_SUM_THRESHOLD = 5000.0; // Total amount above $5000 in window is suspicious

    public static void main(String[] args) {
        // Configure the Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Create the topology
        StreamsBuilder builder = new StreamsBuilder();

        // Create the state store
        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("fraud-detector-state"),
                Serdes.String(),
                new UserActivitySerde()
            )
        );

        // Read from the source topic
        KStream<String, String> transactionStream = builder.stream(
                SOURCE_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Process transactions and detect fraud using a stateful transformer
        transactionStream
            .transformValues(FraudDetector::new, "fraud-detector-state")
            .filter((key, alert) -> alert != null)
            .to(ALERTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // Create the Kafka Streams instance
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Set up shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            System.out.println("State changed from " + oldState + " to " + newState);
            if (newState == KafkaStreams.State.RUNNING) {
                System.out.println("Fraud detection stream processing started");
            }
        });

        try {
            createAlertsTopic();
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            System.err.println("Stream processing interrupted: " + e.getMessage());
        } finally {
            streams.close();
        }
    }

    private static void createAlertsTopic() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            int partitions = 3;
            short replicationFactor = 3;

            NewTopic topic = new NewTopic(ALERTS_TOPIC, partitions, replicationFactor);

            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(topic));

            try {
                KafkaFuture<Void> future = result.all();
                future.get(); // This blocks until topic creation is complete
                System.out.println("Alerts topic created successfully: " + ALERTS_TOPIC);
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println("Alerts topic already exists: " + ALERTS_TOPIC);
                } else {
                    System.err.println("Error creating alerts topic: " + e.getMessage());
                }
            }
        }
    }

    // Stateful transformer to detect fraud
    private static class FraudDetector implements ValueTransformer<String, String> {
        private KeyValueStore<String, UserActivity> userActivityStore;

        @Override
        public void init(ProcessorContext context) {
            userActivityStore = context.getStateStore("fraud-detector-state");
        }

        @Override
        public String transform(String transactionJson) {
            try {
                Transaction transaction = objectMapper.readValue(transactionJson, Transaction.class);

                // Skip low-value transactions
                if (transaction.getAmount() <= HIGH_VALUE_THRESHOLD) {
                    return null;
                }

                String userId = transaction.getUserId();

                // Get user activity from state store or create a new one
                UserActivity activity = userActivityStore.get(userId);
                if (activity == null) {
                    activity = new UserActivity();
                }

                // Update user activity
                activity.addTransaction(transaction);

                // Store updated activity back to the state store
                userActivityStore.put(userId, activity);

                // Check for suspicious activity
                if (activity.isHighRiskActivity()) {
                    // Generate email-like alert
                    String alert = generateAlert(userId, activity, transaction);

                    // Print the alert to console
                    System.out.println(alert);

                    return alert;
                }

                return null;
            } catch (IOException e) {
                System.err.println("Error processing transaction: " + e.getMessage());
                return null;
            }
        }

        @Override
        public void close() {
            // No resources to clean up
        }

        private String generateAlert(String userId, UserActivity activity, Transaction transaction) {
            StringBuilder alert = new StringBuilder();
            alert.append("\n========== FRAUD ALERT ==========\n");
            alert.append("To: fraud-team@bank.com\n");
            alert.append("From: fraud-detection-system@bank.com\n");
            alert.append("Subject: Suspicious Activity Detected for User ").append(userId).append("\n");
            alert.append("Body:\n");
            alert.append("Suspicious activity has been detected for user ").append(userId).append("\n");
            alert.append("Number of high-value transactions in last 10 minutes: ").append(activity.getCount()).append("\n");
            alert.append("Total amount in last 10 minutes: $").append(String.format("%.2f", activity.getTotalAmount())).append("\n");
            alert.append("Last transaction amount: $").append(String.format("%.2f", transaction.getAmount())).append("\n");
            alert.append("Last transaction location: ").append(transaction.getLocation()).append("\n");
            alert.append("Last transaction device: ").append(transaction.getDeviceId()).append("\n");
            alert.append("Last transaction time: ").append(transaction.getTimestamp()).append("\n");
            alert.append("Please investigate immediately.\n");
            alert.append("================================\n");
            return alert.toString();
        }
    }


}
