-- Create a stream from the transactions topic
CREATE STREAM transactions_stream (
    userId VARCHAR,
    amount DOUBLE,
    location VARCHAR,
    timestamp VARCHAR,
    deviceId VARCHAR
) WITH (
    KAFKA_TOPIC='transactions',
    VALUE_FORMAT='JSON'
);

-- Create a stream of high-value transactions (potential fraud)
CREATE STREAM high_value_transactions AS
    SELECT 
        userId,
        amount,
        location,
        SPLIT(location, ', ')[1] AS country,
        timestamp,
        deviceId
    FROM transactions_stream
    WHERE amount > 1000.0
    EMIT CHANGES;

-- Create a table that counts fraud by country
CREATE TABLE fraud_by_country AS
    SELECT 
        country,
        COUNT(*) AS fraud_count,
        SUM(amount) AS total_fraud_amount
    FROM high_value_transactions
    GROUP BY country
    EMIT CHANGES;

-- Create a table that ranks countries by fraud count
CREATE TABLE country_fraud_ranking AS
    SELECT 
        country,
        fraud_count,
        total_fraud_amount,
        TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS window_start,
        TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS window_end
    FROM fraud_by_country
    WINDOW TUMBLING (SIZE 1 HOUR)
    EMIT CHANGES;

-- Print the results to the console
SELECT 
    country,
    fraud_count,
    total_fraud_amount,
    window_start,
    window_end
FROM country_fraud_ranking
ORDER BY fraud_count DESC
EMIT CHANGES;