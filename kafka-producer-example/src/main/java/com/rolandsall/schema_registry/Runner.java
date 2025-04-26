package com.rolandsall.schema_registry;

import com.rolandsall.schema_registry.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Runner {

    public static final String BOOTSTRAP_SERVER = "localhost:19092";
    public static final String TOPIC_NAME = "user";

    public static void main(String[] args) {
        createTopics();
        produceWithRegistry();
    }

    private static void produceWithRegistry() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);

        try {
            // Create a sample user
            User user = new User();
            user.setName("John Doe");
            user.setAge(30);
            // commenting the email will cause a schema problem
            user.setEmail("john.doe@example.com");

            String key = user.getName();

            ProducerRecord<String, User> record = new ProducerRecord<>(TOPIC_NAME, key, user);

            System.out.println("Sending user: " + user);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Record sent successfully! Topic: " +
                            metadata.topic() + ", Partition: " +
                            metadata.partition() + ", Offset: " +
                            metadata.offset());
                } else {
                    System.err.println("Error producing record: " + exception.getMessage());
                    exception.printStackTrace();
                }
            });


        } catch (Exception e) {
            System.err.println("Error producing records: " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static void createTopics() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 1000);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Define topic configurations
            int partitions = 3;
            short replicationFactor = 3; // Use 3 in production for fault tolerance

            // Create topic
            NewTopic topic1 = new NewTopic(TOPIC_NAME, partitions, replicationFactor);

            // Create the topics
            CreateTopicsResult result = adminClient.createTopics(
                    List.of(topic1)
            );

            // Wait for topic creation to complete
            try {
                KafkaFuture<Void> future = result.all();
                future.get(); // This blocks until topic creation is complete
                System.out.println("Topics created successfully");
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                    System.out.println("Topics already exist");
                } else {
                    System.err.println("Error creating topics: " + e.getMessage());
                }
            }
        }
    }
}
