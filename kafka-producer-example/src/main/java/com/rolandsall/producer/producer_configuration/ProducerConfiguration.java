package com.rolandsall.producer.producer_configuration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerConfiguration {

    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String TOPIC_NAME = "example-topic";

    public static void main(String[] args) {
        createTopics();
        produceMessages();
    }

    private static void produceMessages() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "MY_PRODUCER_CONFIG_APP");
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 0, 1


        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);


        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            List<PartitionInfo> partitionInfos = producer.partitionsFor(TOPIC_NAME);
            System.out.println("Partition infos: " + partitionInfos);
            // Send 5 messages
            for (int i = 0; i < 5; i++) {
                String key = "key-" + i;
                String value = "message-" + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);

                try {
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("Message sent to partition %d with offset %d%n",
                            metadata.partition(), metadata.offset());
                } catch (InterruptedException | ExecutionException e) {
                    System.err.println("Error sending message: " + e.getMessage());
                }
            }
            System.out.println("All messages sent successfully");
        }
    }

    private static void createTopics() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

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
