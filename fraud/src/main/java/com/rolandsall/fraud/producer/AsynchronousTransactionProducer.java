package com.rolandsall.fraud.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rolandsall.fraud.model.Transaction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class AsynchronousTransactionProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String TOPIC_NAME = "transactions";
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final Random random = new Random();

    public static void main(String[] args) {
        createTopics();
        produceTransactions();
    }

    private static void produceTransactions() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Send transactions continuously with a small delay
            int count = 0;
            while (count < 100) { // Limit to 100 transactions for testing
                Transaction transaction = generateRandomTransaction();
                String userId = transaction.getUserId();

                try {
                    String transactionJson = objectMapper.writeValueAsString(transaction);
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, userId, transactionJson);
                    producer.send(record, new TransactionProducerCallback());
                    System.out.println("Transaction sent: " + transaction);
                    
                    // Add some fraudulent transactions occasionally
                    if (count % 10 == 0) {
                        Transaction fraudTransaction = generateFraudulentTransaction();
                        String fraudJson = objectMapper.writeValueAsString(fraudTransaction);
                        ProducerRecord<String, String> fraudRecord = new ProducerRecord<>(TOPIC_NAME, fraudTransaction.getUserId(), fraudJson);
                        producer.send(fraudRecord, new TransactionProducerCallback());
                        System.out.println("Fraudulent transaction sent: " + fraudTransaction);
                    }
                    
                    Thread.sleep(1000); // 1 second delay between transactions
                } catch (Exception e) {
                    System.err.println("Error sending transaction: " + e.getMessage());
                }
                count++;
            }

            // Make sure to flush to send all records
            producer.flush();
            System.out.println("All transactions sent successfully");
        }
    }

    private static Transaction generateRandomTransaction() {
        String userId = "user-" + (random.nextInt(5) + 1);
        double amount = Math.round(random.nextDouble() * 500 * 100.0) / 100.0; // Random amount up to $500
        String location = generateRandomLocation();
        LocalDateTime timestamp = LocalDateTime.now();
        String deviceId = "device-" + (random.nextInt(5) + 1);

        return new Transaction(userId, amount, location, timestamp, deviceId);
    }

    private static Transaction generateFraudulentTransaction() {
        // Generate a transaction with suspicious characteristics
        String userId = "user-" + (random.nextInt(5) + 1);
        double amount = Math.round((random.nextDouble() * 5000 + 1000) * 100.0) / 100.0; // High amount $1000-$6000
        String location = generateFraudulentLocation();
        LocalDateTime timestamp = LocalDateTime.now();
        String deviceId = "device-" + (random.nextInt(100) + 50); // Unusual device

        return new Transaction(userId, amount, location, timestamp, deviceId);
    }

    private static String generateRandomLocation() {
        String[] countries = {"USA", "Canada", "UK", "France", "Germany", "Japan", "Australia"};
        String[] cities = {"New York", "Toronto", "London", "Paris", "Berlin", "Tokyo", "Sydney"};
        
        int index = random.nextInt(countries.length);
        return cities[index] + ", " + countries[index];
    }

    private static String generateFraudulentLocation() {
        // Use locations that might be suspicious for fraud
        String[] suspiciousCountries = {"Nigeria", "Russia", "Ukraine", "North Korea", "Belarus"};
        String[] suspiciousCities = {"Lagos", "Moscow", "Kiev", "Pyongyang", "Minsk"};
        
        int index = random.nextInt(suspiciousCountries.length);
        return suspiciousCities[index] + ", " + suspiciousCountries[index];
    }

    private static void createTopics() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            int partitions = 3;
            short replicationFactor = 3; // Use 3 in production for fault tolerance

            NewTopic topic = new NewTopic(TOPIC_NAME, partitions, replicationFactor);

            CreateTopicsResult result = adminClient.createTopics(List.of(topic));

            try {
                KafkaFuture<Void> future = result.all();
                future.get(); // This blocks until topic creation is complete
                System.out.println("Topic created successfully: " + TOPIC_NAME);
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println("Topic already exists: " + TOPIC_NAME);
                } else {
                    System.err.println("Error creating topic: " + e.getMessage());
                }
            }
        }
    }
}