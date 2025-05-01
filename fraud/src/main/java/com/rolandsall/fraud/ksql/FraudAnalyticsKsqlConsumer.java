package com.rolandsall.fraud.ksql;

import io.confluent.ksql.api.client.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudAnalyticsKsqlConsumer {
    private static final Logger logger = LoggerFactory.getLogger(FraudAnalyticsKsqlConsumer.class);
    private static final String KSQLDB_SERVER_HOST = "localhost";
    private static final int KSQLDB_SERVER_PORT = 8088;

    public static void main(String[] args) {
        // Create the ksqlDB client
        ClientOptions options = ClientOptions.create()
                .setHost(KSQLDB_SERVER_HOST)
                .setPort(KSQLDB_SERVER_PORT);

        Client client = Client.create(options);

        try {
            // Create the transactions stream
            executeStatement(client, 
                "CREATE STREAM IF NOT EXISTS transactions_stream (" +
                "    userId VARCHAR," +
                "    amount DOUBLE," +
                "    location VARCHAR," +
                "    timestamp VARCHAR," +
                "    deviceId VARCHAR" +
                ") WITH (" +
                "    KAFKA_TOPIC='transactions'," +
                "    VALUE_FORMAT='JSON'" +
                ");");

            // Create the high-value transactions stream
            executeStatement(client,
                "CREATE STREAM IF NOT EXISTS high_value_transactions AS" +
                "    SELECT " +
                "        userId," +
                "        amount," +
                "        location," +
                "        SPLIT(location, ', ')[1] AS country," +
                "        timestamp," +
                "        deviceId" +
                "    FROM transactions_stream" +
                "    WHERE amount > 1000.0" +
                "    EMIT CHANGES;");

            // Create the fraud by country table
            executeStatement(client,
                "CREATE TABLE IF NOT EXISTS fraud_by_country AS" +
                "    SELECT " +
                "        country," +
                "        COUNT(*) AS fraud_count," +
                "        SUM(amount) AS total_fraud_amount" +
                "    FROM high_value_transactions" +
                "    GROUP BY country" +
                "    EMIT CHANGES;");

            // Start a push query to continuously receive updates
            logger.info("Starting push query to monitor fraud by country...");
            client.streamQuery("SELECT country, fraud_count, total_fraud_amount FROM fraud_by_country ORDER BY fraud_count DESC EMIT CHANGES;",
                    Collections.emptyMap())
                    .thenAccept(streamedQueryResult -> {
                        System.out.println("\n========== FRAUD ANALYTICS REPORT ==========");
                        System.out.println("Countries with the most fraud transactions:");
                        System.out.println("------------------------------------------");
                        System.out.printf("%-15s %-15s %-20s%n", "COUNTRY", "FRAUD COUNT", "TOTAL FRAUD AMOUNT");
                        System.out.println("------------------------------------------");

                        // Subscribe to the query result
                        streamedQueryResult.subscribe(new Subscriber<Row>() {
                            @Override
                            public void onSubscribe(Subscription subscription) {
                                // Request an unbounded number of items
                                subscription.request(Long.MAX_VALUE);
                            }

                            @Override
                            public void onNext(Row row) {
                                String country = row.getString("COUNTRY");
                                long fraudCount = row.getLong("FRAUD_COUNT");
                                double totalAmount = row.getDouble("TOTAL_FRAUD_AMOUNT");

                                System.out.printf("%-15s %-15d $%-20.2f%n", country, fraudCount, totalAmount);
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                logger.error("Error in subscription", throwable);
                            }

                            @Override
                            public void onComplete() {
                                logger.info("Subscription completed");
                            }
                        });
                    }).exceptionally(e -> {
                        logger.error("Error executing push query", e);
                        return null;
                    });

            // Keep the application running
            Thread.sleep(Long.MAX_VALUE);

        } catch (Exception e) {
            logger.error("Error in ksqlDB client", e);
        } finally {
            // Close the client
            client.close();
        }
    }

    private static void executeStatement(Client client, String statement) {
        try {
            logger.info("Executing statement: {}", statement);
            ExecuteStatementResult result = client.executeStatement(statement, Collections.emptyMap()).get();
            logger.info("Statement executed successfully: {}", result.queryId());
        } catch (InterruptedException | ExecutionException e) {
            if (e.getMessage().contains("already exists")) {
                logger.info("Stream or table already exists, continuing...");
            } else {
                logger.error("Error executing statement", e);
                throw new RuntimeException("Error executing ksqlDB statement", e);
            }
        }
    }
}
