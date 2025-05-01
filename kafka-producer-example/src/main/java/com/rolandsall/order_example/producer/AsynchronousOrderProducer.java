package com.rolandsall.order_example.producer;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.TopicExistsException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.Random;

public class AsynchronousOrderProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String TOPIC_NAME = "orders-topic";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();

    public static void main(String[] args) {
        createTopics();
        produceOrders();
    }

    private static void produceOrders() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Send 5 orders
            for (int i = 0; i < 5; i++) {
                // Create an order with random data
                Order order = generateRandomOrder();
                String orderId = order.getOrderId();

                try {
                    String orderJson = objectMapper.writeValueAsString(order);

                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, orderId, orderJson);

                    producer.send(record, new OrderProducerCallback());

                    System.out.println("Order sent: " + order);
                } catch (JsonProcessingException e) {
                    System.err.println("Error serializing order to JSON: " + e.getMessage());
                }
            }

            // Make sure to flush to send all records
            producer.flush();
            System.out.println("All orders sent successfully");
        }
    }

    private static Order generateRandomOrder() {
        String orderId = UUID.randomUUID().toString();
        String userId = "user-" + (random.nextInt(1000) + 1);
        double totalPrice = Math.round(random.nextDouble() * 1000 * 100.0) / 100.0; // Random price up to $1000 with 2 decimal places
        String address = generateRandomAddress();

        return new Order(orderId, userId, totalPrice, address);
    }

    private static String generateRandomAddress() {
        String[] streets = {"Main St", "Oak Ave"};
        String[] cities = {"New York", "Los Angeles"};
        String[] states = {"NY", "CA",};

        int streetNum = random.nextInt(1000) + 1;
        String street = streets[random.nextInt(streets.length)];
        String city = cities[random.nextInt(cities.length)];
        String state = states[random.nextInt(states.length)];
        int zipCode = 10000 + random.nextInt(90000);

        return streetNum + " " + street + ", " + city + ", " + state + " " + zipCode;
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

    public static class Order {
        private String orderId;
        private String userId;
        private double totalPrice;
        private String address;

        // Default constructor for Jackson
        public Order() {}

        public Order(String orderId, String userId, double totalPrice, String address) {
            this.orderId = orderId;
            this.userId = userId;
            this.totalPrice = totalPrice;
            this.address = address;
        }

        // Getters and setters
        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public double getTotalPrice() {
            return totalPrice;
        }

        public void setTotalPrice(double totalPrice) {
            this.totalPrice = totalPrice;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "orderId='" + orderId + '\'' +
                    ", userId='" + userId + '\'' +
                    ", totalPrice=" + totalPrice +
                    ", address='" + address + '\'' +
                    '}';
        }
    }
}